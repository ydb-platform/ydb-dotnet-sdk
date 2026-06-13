using System.Diagnostics;
using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Tracing;

[Collection("DisableParallelization")]
public class YdbExecutionStrategyTracingTests
{
    private const string FakeConnectionString = "Host=localhost;Port=2136;Database=/test";

    [Fact]
    public async Task ExecuteAsync_FirstAttemptSucceeds_EmitsRunWithRetryAndOneTry()
    {
        await using var db = CreateContext();
        using var listener = StartListener(out var activities);

        var strategy = db.Database.CreateExecutionStrategy();
        var result = await strategy.ExecuteAsync(
            state: 0,
            operation: (_, _, _) => Task.FromResult(42),
            verifySucceeded: null);

        Assert.Equal(42, result);

        var run = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Equal(ActivityKind.Internal, run.Kind);
        Assert.Empty(run.TagObjects);

        var tryActivity = Assert.Single(activities, a => a.DisplayName == "ydb.Try");
        Assert.Equal(ActivityKind.Internal, tryActivity.Kind);
        Assert.Equal(ActivityStatusCode.Unset, tryActivity.Status);
        Assert.Empty(tryActivity.TagObjects);
    }

    [Fact]
    public async Task Execute_FirstAttemptSucceeds_EmitsRunWithRetryAndOneTry()
    {
        await using var db = CreateContext();
        using var listener = StartListener(out var activities);

        var strategy = db.Database.CreateExecutionStrategy();
        var result = strategy.Execute(
            state: 0,
            operation: (_, _) => 7,
            verifySucceeded: null);

        Assert.Equal(7, result);

        var run = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Equal(ActivityKind.Internal, run.Kind);

        var tryActivity = Assert.Single(activities, a => a.DisplayName == "ydb.Try");
        Assert.Equal(ActivityStatusCode.Unset, tryActivity.Status);
        Assert.Null(tryActivity.GetTagItem("ydb.retry.backoff_ms"));
    }

    [Fact]
    public async Task ExecuteAsync_RetryThenSuccess_EmitsTwoTryActivitiesWithBackoffOnRetry()
    {
        await using var db = CreateContext(b => b.UseRetryPolicy(FastRetryConfig));
        using var listener = StartListener(out var activities);

        var strategy = db.Database.CreateExecutionStrategy();
        var firstAttempt = true;
        await strategy.ExecuteAsync(
            state: 0,
            operation: (_, _, _) =>
            {
                if (!firstAttempt)
                    return Task.FromResult(0);
                firstAttempt = false;
                throw new YdbException(StatusCode.Aborted, "TLI!");
            },
            verifySucceeded: null);

        var run = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Equal(ActivityKind.Internal, run.Kind);
        Assert.Equal(ActivityStatusCode.Unset, run.Status);

        var tryActivities = activities.Where(a => a.DisplayName == "ydb.Try").ToList();
        Assert.Equal(2, tryActivities.Count);
        AssertTryActivitiesAreChildrenOf(run, tryActivities);

        // First ydb.Try: failed attempt, no backoff attribute, error tags from YdbException.
        var firstTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") == null);
        Assert.Equal(ActivityStatusCode.Error, firstTry.Status);
        Assert.Equal(StatusCode.Aborted, firstTry.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", firstTry.GetTagItem("error.type"));

        // Second ydb.Try: successful retry, has backoff attribute, no error.
        var retryTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") != null);
        Assert.Equal(ActivityStatusCode.Unset, retryTry.Status);
        Assert.NotNull(retryTry.GetTagItem("ydb.retry.backoff_ms"));
    }

    [Fact]
    public async Task ExecuteAsync_NonRetryableError_EmitsRunWithRetryAndTryWithError()
    {
        await using var db = CreateContext();
        using var listener = StartListener(out var activities);

        var strategy = db.Database.CreateExecutionStrategy();

        // ShouldRetryOn returns false for non-retryable YdbException codes, so EF rethrows the original exception.
        await Assert.ThrowsAsync<YdbException>(() =>
            strategy.ExecuteAsync(
                state: 0,
                operation: (_, _, _) => Task.FromException<int>(
                    new YdbException(StatusCode.Unauthorized, "no access")),
                verifySucceeded: null));

        var run = GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal(StatusCode.Unauthorized, run.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", run.GetTagItem("error.type"));

        var tryActivity = GetSingleActivity(activities, "ydb.Try", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Null(tryActivity.GetTagItem("ydb.retry.backoff_ms"));
        Assert.Equal(StatusCode.Unauthorized, tryActivity.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", tryActivity.GetTagItem("error.type"));
    }

    [Fact]
    public async Task ExecuteAsync_NonYdbException_DoesNotRetryAndRethrowsOriginal()
    {
        await using var db = CreateContext(b => b.UseRetryPolicy(FastRetryConfig));
        using var listener = StartListener(out var activities);

        var strategy = db.Database.CreateExecutionStrategy();
        var calls = 0;

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            strategy.ExecuteAsync(
                state: 0,
                operation: (_, _, _) =>
                {
                    calls++;
                    return Task.FromException<int>(new InvalidOperationException("boom"));
                },
                verifySucceeded: null));

        Assert.Equal("boom", exception.Message);
        Assert.Equal(1, calls);

        var run = GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal(typeof(InvalidOperationException).FullName, run.GetTagItem("error.type"));

        var tryActivity = GetSingleActivity(activities, "ydb.Try", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Null(tryActivity.GetTagItem("ydb.retry.backoff_ms"));
        Assert.Null(tryActivity.GetTagItem("db.response.status_code"));
        Assert.Equal(typeof(InvalidOperationException).FullName, tryActivity.GetTagItem("error.type"));
    }

    [Fact]
    public async Task ExecuteAsync_RetriesExhausted_EmitsErrorOnAllSpans()
    {
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 2,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1
        }));
        using var listener = StartListener(out var activities);

        var strategy = db.Database.CreateExecutionStrategy();

        // RetryLimitExceededException wraps the last YdbException.
        await Assert.ThrowsAsync<RetryLimitExceededException>(() =>
            strategy.ExecuteAsync(
                state: 0,
                operation: (_, _, _) => Task.FromException<int>(
                    new YdbException(StatusCode.Aborted, "always fails")),
                verifySucceeded: null));

        // Outer span gets the wrapper exception type.
        var run = GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal(typeof(RetryLimitExceededException).FullName, run.GetTagItem("error.type"));

        var tryActivities = activities.Where(a => a.DisplayName == "ydb.Try").ToList();
        Assert.Equal(2, tryActivities.Count);
        Assert.All(tryActivities, a => Assert.Equal(ActivityStatusCode.Error, a.Status));
        AssertTryActivitiesAreChildrenOf(run, tryActivities);

        // First ydb.Try: no backoff attribute, has YdbException error tags.
        var firstTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") == null);
        Assert.Equal(StatusCode.Aborted, firstTry.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", firstTry.GetTagItem("error.type"));

        // Retry ydb.Try: GetNextDelay returns null on retry-exhausted, so it's never closed
        // by the strategy and ends up tagged with the wrapper RetryLimitExceededException
        // set in the outer catch.
        var retryTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") != null);
        Assert.NotNull(retryTry.GetTagItem("ydb.retry.backoff_ms"));
        Assert.Equal(typeof(RetryLimitExceededException).FullName, retryTry.GetTagItem("error.type"));
    }

    private static YdbRetryPolicyConfig FastRetryConfig => new()
    {
        MaxAttempts = 5,
        FastBackoffBaseMs = 1,
        FastCapBackoffMs = 1
    };

    private static TestDbContext CreateContext(Action<YdbDbContextOptionsBuilder>? configure = null) =>
        new(configure);

    private sealed class TestDbContext(Action<YdbDbContextOptionsBuilder>? configure) : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseYdb(FakeConnectionString, ydb => configure?.Invoke(ydb));
    }

    private static ActivityListener StartListener(out List<Activity> activities)
    {
        var captured = new List<Activity>();
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Ydb.Sdk",
            // ReSharper disable once RedundantLambdaParameterType
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => captured.Add(activity)
        };

        ActivitySource.AddActivityListener(listener);
        activities = captured;
        return listener;
    }

    private static Activity GetSingleActivity(
        List<Activity> activities,
        string expectedDisplayName,
        ActivityStatusCode? expectedStatusCode = ActivityStatusCode.Unset)
    {
        var filtered = activities.Where(a => a.DisplayName == expectedDisplayName).ToList();
        Assert.Single(filtered);

        var activity = filtered[0];
        Assert.Equal(expectedStatusCode ?? ActivityStatusCode.Unset, activity.Status);
        return activity;
    }

    private static void AssertTryActivitiesAreChildrenOf(Activity runActivity, IEnumerable<Activity> tryActivities)
    {
        foreach (var tryActivity in tryActivities)
            Assert.Equal(runActivity.Id, tryActivity.ParentId);
    }
}
