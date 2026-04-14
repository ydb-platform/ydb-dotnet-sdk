using System.Diagnostics;
using Xunit;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests.Tracing;

[Collection("DisableParallelization")]
public class YdbTracingTests : TestBase
{
    private static readonly YdbConnectionStringBuilder ConnectionSettings = new(TestUtils.ConnectionString);

    [Fact]
    public async Task CreateSession_EmitsActivity_WhenPoolCleared()
    {
        await YdbConnection.ClearAllPools();
        using var activityListener = StartListener(out var activities);

        await using var connection = await CreateOpenConnectionAsync();
        await new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync();

        var driverInitActivity = GetSingleActivity(activities, "ydb.Driver.Initialize");
        Assert.Equal(ActivityKind.Internal, driverInitActivity.Kind);
        Assert.Empty(driverInitActivity.TagObjects);
        Assert.Empty(driverInitActivity.Events);

        var activity = GetSingleActivity(activities, "ydb.CreateSession");
        Assert.Equal(ActivityKind.Client, activity.Kind);
        Assert.Empty(activity.Events);
        AssertCommonDbTags(activity);
    }

    [Fact]
    public async Task CommandExecute_DoesNotEmitYdbActivity_WhenYdbActivitySourceIsNotEnabled()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener();
        listener.ShouldListenTo = source => source.Name == "test.source";
        listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded;
        listener.ActivityStopped = activity => activities.Add(activity);
        ActivitySource.AddActivityListener(listener);

        using var testSource = new ActivitySource("test.source");
        // ReSharper disable once ExplicitCallerInfoArgument
        using (testSource.StartActivity("test.parent"))
        {
            await using var connection = await CreateOpenConnectionAsync();
            await new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync();
        }

        var capturedNames = activities.Select(a => a.DisplayName).ToList();
        Assert.Contains("test.parent", capturedNames);
        Assert.DoesNotContain(capturedNames, name => name.StartsWith("ydb.", StringComparison.Ordinal));
    }

    [Fact]
    public async Task CommandExecute_DoesNotWriteYdbData_ToExternalActivity()
    {
        using var listener = new ActivityListener();
        listener.ShouldListenTo = source => source.Name == "test.source";
        listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded;
        ActivitySource.AddActivityListener(listener);

        using var testSource = new ActivitySource("test.source");
        // ReSharper disable once ExplicitCallerInfoArgument
        using var parent = testSource.StartActivity("test.parent");
        Assert.NotNull(parent);

        await using var connection = await CreateOpenConnectionAsync();
        await new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync();

        var parentTags = parent.TagObjects.Select(t => t.Key).ToList();
        Assert.Equal(ActivityStatusCode.Unset, parent.Status);
        Assert.DoesNotContain(parentTags, key => key.StartsWith("db.", StringComparison.Ordinal));
        Assert.DoesNotContain(parentTags, key => key.StartsWith("server.", StringComparison.Ordinal));
        Assert.DoesNotContain(parentTags, key => key.StartsWith("network.", StringComparison.Ordinal));
        Assert.DoesNotContain(parentTags, key => key.StartsWith("error.", StringComparison.Ordinal));
    }

    [Fact]
    public async Task CommandExecute_WhenActivityAllDataIsDisabled_EmitsActivityWithoutTags()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener();
        listener.ShouldListenTo = source => source.Name == "Ydb.Sdk";
        listener.Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.PropagationData;
        listener.ActivityStopped = activity => activities.Add(activity);
        ActivitySource.AddActivityListener(listener);

        await using var connection = await CreateOpenConnectionAsync();
        await new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync();

        var activity = GetSingleActivity(activities, "ydb.ExecuteQuery");
        Assert.False(activity.IsAllDataRequested);
        Assert.Empty(activity.TagObjects);
    }

    [Fact]
    public async Task CommandExecute_EmitsActivityWithDbTags()
    {
        await using var connection = await CreateOpenConnectionAsync();
        using var activityListener = StartListener(out var activities);
        _ = await new YdbCommand("SELECT 42;", connection).ExecuteScalarAsync();

        var activity = GetSingleActivity(activities, "ydb.ExecuteQuery");
        Assert.Empty(activity.Events);
        AssertCommonDbTags(activity);
    }

    [Fact]
    public async Task CommandExecute_Error_SetsErrorStatusAndTags()
    {
        await using var connection = await CreateOpenConnectionAsync();
        using var activityListener = StartListener(out var activities);

        _ = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await new YdbCommand("SELECT * FROM non_existing_table", connection).ExecuteScalarAsync());

        var activity = GetSingleActivity(activities, "ydb.ExecuteQuery", expectedStatusCode: ActivityStatusCode.Error);
        Assert.NotNull(activity.StatusDescription);

        var tags = activity.TagObjects.ToDictionary(t => t.Key, t => t.Value);
        Assert.Contains("error.type", tags.Keys);
    }

    [Fact]
    public async Task Commit_EmitsActivity()
    {
        await using var connection = await CreateOpenConnectionAsync();
        using var activityListener = StartListener(out var activities);

        var tx = connection.BeginTransaction();
        _ = await new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync();
        await tx.CommitAsync();

        var activity = GetSingleActivity(activities, "ydb.Commit");
        Assert.Empty(activity.Events);
        AssertCommonDbTags(activity);
    }

    [Fact]
    public async Task Rollback_EmitsActivity()
    {
        await using var connection = await CreateOpenConnectionAsync();
        using var activityListener = StartListener(out var activities);

        var tx = connection.BeginTransaction();
        await new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync();
        await tx.RollbackAsync();

        var activity = GetSingleActivity(activities, "ydb.Rollback");
        Assert.Empty(activity.Events);
        AssertCommonDbTags(activity);
    }

    [Fact]
    public async Task ExecuteInTransaction_EmitsActivity()
    {
        await using var ydbDataSource = new YdbDataSource(ConnectionString);

        // init driver
        await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();
        await new YdbCommand("SELECT 1;", ydbConnection).ExecuteNonQueryAsync();

        using var activityListener = StartListener(out var activities);

        await ydbDataSource.ExecuteInTransactionAsync(connection =>
            new YdbCommand("SELECT 1;", connection).ExecuteNonQueryAsync());

        var executeWithRetryActivity = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Empty(executeWithRetryActivity.Events);
        Assert.Equal(ActivityKind.Internal, executeWithRetryActivity.Kind);
        Assert.Empty(executeWithRetryActivity.TagObjects);

        var tryActivity = GetSingleActivity(activities, "ydb.Try");
        Assert.Empty(tryActivity.TagObjects);
        Assert.Equal(ActivityKind.Internal, tryActivity.Kind);

        var executeQueryActivity = GetSingleActivity(activities, "ydb.ExecuteQuery");
        Assert.Empty(executeQueryActivity.Events);
        Assert.Equal(ActivityKind.Client, executeQueryActivity.Kind);
        AssertCommonDbTags(executeQueryActivity);

        var commitActivity = GetSingleActivity(activities, "ydb.Commit");
        Assert.Empty(commitActivity.Events);
        Assert.Equal(ActivityKind.Client, commitActivity.Kind);
        AssertCommonDbTags(commitActivity);
    }

    [Fact]
    public async Task RetryableConnection_EmitsActivity()
    {
        await using var ydbDataSource = new YdbDataSource(ConnectionString);

        // init driver
        await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();
        await new YdbCommand("SELECT 1;", ydbConnection).ExecuteNonQueryAsync();

        using var activityListener = StartListener(out var activities);

        await using var ydbRetryableConnection = await ydbDataSource.OpenRetryableConnectionAsync();
        await new YdbCommand("SELECT 1;", ydbRetryableConnection).ExecuteNonQueryAsync();

        var executeWithRetryActivity = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Empty(executeWithRetryActivity.Events);
        Assert.Equal(ActivityKind.Internal, executeWithRetryActivity.Kind);
        Assert.Empty(executeWithRetryActivity.TagObjects);

        var tryActivity = GetSingleActivity(activities, "ydb.Try");
        Assert.Empty(tryActivity.TagObjects);
        Assert.Equal(ActivityKind.Internal, tryActivity.Kind);

        var executeQueryActivity = GetSingleActivity(activities, "ydb.ExecuteQuery");
        Assert.Empty(executeQueryActivity.Events);
        Assert.Equal(ActivityKind.Client, executeQueryActivity.Kind);
        Assert.Equal(true, executeQueryActivity.GetTagItem("ydb.execute.in_memory"));
        AssertCommonDbTags(executeQueryActivity);
    }

    [Fact]
    public async Task Execute_NoRetry_EmitsSingleExecuteAndTryActivity()
    {
        using var activityListener = StartListener(out var activities);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);
        await executor.ExecuteAsync(_ => Task.CompletedTask);

        var executeActivity = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Equal(ActivityKind.Internal, executeActivity.Kind);
        Assert.Empty(executeActivity.TagObjects);

        // First attempt is always wrapped in ydb.Try with no retry attributes
        var tryActivity = GetSingleActivity(activities, "ydb.Try");
        Assert.Equal(ActivityKind.Internal, tryActivity.Kind);
        Assert.Empty(tryActivity.TagObjects);
    }

    [Fact]
    public async Task Execute_WithRetry_EmitsTwoTryActivities()
    {
        using var activityListener = StartListener(out var activities);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);

        var firstAttempt = true;
        await executor.ExecuteAsync(_ =>
        {
            if (!firstAttempt)
                return Task.CompletedTask;

            firstAttempt = false;
            throw new YdbException(StatusCode.Aborted, "TLI!");
        });

        var executeActivity = GetSingleActivity(activities, "ydb.RunWithRetry");
        Assert.Equal(ActivityKind.Internal, executeActivity.Kind);
        Assert.Empty(executeActivity.TagObjects);

        var tryActivities = activities.Where(a => a.DisplayName == "ydb.Try").ToList();
        Assert.Equal(2, tryActivities.Count);

        // First ydb.Try: the failing attempt — has error, no backoff attribute
        var firstTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") == null);
        Assert.Equal(ActivityStatusCode.Error, firstTry.Status);
        Assert.Equal(StatusCode.Aborted, firstTry.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", firstTry.GetTagItem("error.type"));

        // Second ydb.Try: the successful retry — has backoff attribute, no error
        var retryTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") != null);
        Assert.Equal(ActivityStatusCode.Unset, retryTry.Status);
        Assert.NotNull(retryTry.GetTagItem("ydb.retry.backoff_ms"));
    }

    [Fact]
    public async Task Execute_NonRetryableError_EmitsExecuteAndTryWithError()
    {
        using var activityListener = StartListener(out var activities);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);

        await Assert.ThrowsAsync<YdbException>(() =>
            executor.ExecuteAsync(_ => throw new YdbException(StatusCode.Unauthorized, "no access")));

        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal(StatusCode.Unauthorized, executeActivity.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", executeActivity.GetTagItem("error.type"));

        var tryActivity = GetSingleActivity(activities, "ydb.Try", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal(StatusCode.Unauthorized, tryActivity.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", tryActivity.GetTagItem("error.type"));
    }

    [Fact]
    public async Task Execute_RetriesExhausted_EmitsTwoTryActivitiesWithError()
    {
        using var activityListener = StartListener(out var activities);

        var policy = new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 2 });
        var executor = new YdbRetryPolicyExecutor(policy);

        // Aborted is retryable; with MaxAttempts=2 we get 1 retry then failure
        await Assert.ThrowsAsync<YdbException>(() =>
            executor.ExecuteAsync(_ => throw new YdbException(StatusCode.Aborted, "always fails")));

        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal(StatusCode.Aborted, executeActivity.GetTagItem("db.response.status_code"));

        var tryActivities = activities.Where(a => a.DisplayName == "ydb.Try").ToList();
        Assert.Equal(2, tryActivities.Count);
        Assert.All(tryActivities, a => Assert.Equal(ActivityStatusCode.Error, a.Status));

        // First ydb.Try: no backoff attribute, has error
        var firstTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") == null);
        Assert.Equal(StatusCode.Aborted, firstTry.GetTagItem("db.response.status_code"));

        // Second ydb.Try: backoff attribute present, also has error
        var retryTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") != null);
        Assert.NotNull(retryTry.GetTagItem("ydb.retry.backoff_ms"));
        Assert.Equal(StatusCode.Aborted, retryTry.GetTagItem("db.response.status_code"));
    }

    [Fact]
    public async Task Execute_CancellationDuringOperation_SetsErrorTypeOnTryAndExecuteSpans()
    {
        using var activityListener = StartListener(out var activities);
        using var cts = new CancellationTokenSource();

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            executor.ExecuteAsync(_ =>
            {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            }, cts.Token));

        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal("System.OperationCanceledException", executeActivity.GetTagItem("error.type"));

        var tryActivity = GetSingleActivity(activities, "ydb.Try", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal("System.OperationCanceledException", tryActivity.GetTagItem("error.type"));
    }

    [Fact]
    public async Task Execute_CancellationDuringRetryDelay_SetsErrorTypeOnExecuteSpan()
    {
        using var activityListener = StartListener(out var activities);
        using var cts = new CancellationTokenSource();

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);
        var firstAttempt = true;
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            executor.ExecuteAsync(_ =>
            {
                if (!firstAttempt)
                    return Task.CompletedTask; // should never reach here
                firstAttempt = false;
                cts.Cancel(); // delay after this will throw OCE
                throw new YdbException(StatusCode.Aborted, "retry me");
            }, cts.Token));

        // ydb.RunWithRetry gets OCE error type (from the cancelled delay)
        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal("System.OperationCanceledException", executeActivity.GetTagItem("error.type"));

        // Two ydb.Try spans: the first attempt (Aborted) and the retry span that was opened before
        // the delay but received OCE when the delay was cancelled
        var tryActivities = activities.Where(a => a.DisplayName == "ydb.Try").ToList();
        Assert.Equal(2, tryActivities.Count);
        Assert.All(tryActivities, a => Assert.Equal(ActivityStatusCode.Error, a.Status));

        var firstTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") == null);
        Assert.Equal(StatusCode.Aborted, firstTry.GetTagItem("db.response.status_code"));
        Assert.Equal("ydb_error", firstTry.GetTagItem("error.type"));

        var retryTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") != null);
        Assert.Equal("System.OperationCanceledException", retryTry.GetTagItem("error.type"));
    }

    [Fact]
    public async Task Execute_CancellationDuringRetryOperation_SetsErrorOnBothSpans()
    {
        using var activityListener = StartListener(out var activities);
        using var cts = new CancellationTokenSource();

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);
        var firstAttempt = true;
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            executor.ExecuteAsync(ct =>
            {
                if (!firstAttempt)
                {
                    // Second attempt (retry): cancel and throw OCE
                    cts.Cancel();
                    ct.ThrowIfCancellationRequested();
                }
                firstAttempt = false;
                throw new YdbException(StatusCode.Aborted, "retry me");
            }, cts.Token));

        // ydb.RunWithRetry gets OCE error type
        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal("System.OperationCanceledException", executeActivity.GetTagItem("error.type"));

        var tryActivities = activities.Where(a => a.DisplayName == "ydb.Try").ToList();
        Assert.Equal(2, tryActivities.Count);
        Assert.All(tryActivities, a => Assert.Equal(ActivityStatusCode.Error, a.Status));

        // First ydb.Try: failed with Aborted
        var firstTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") == null);
        Assert.Equal(StatusCode.Aborted, firstTry.GetTagItem("db.response.status_code"));

        // Retry ydb.Try: cancelled with OCE during the retry operation itself
        var retryTry = tryActivities.First(a => a.GetTagItem("ydb.retry.backoff_ms") != null);
        Assert.Equal("System.OperationCanceledException", retryTry.GetTagItem("error.type"));
    }

    [Fact]
    public async Task ExecuteInTransaction_CancellationDuringOperation_SetsErrorOnExecuteAndTrySpans()
    {
        await using var ydbDataSource = new YdbDataSource(ConnectionString);

        // Warm up the driver so session creation doesn't appear in recorded activities
        await using var initConnection = await ydbDataSource.OpenConnectionAsync();
        await new YdbCommand("SELECT 1;", initConnection).ExecuteNonQueryAsync();

        using var activityListener = StartListener(out var activities);
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            ydbDataSource.ExecuteInTransactionAsync(async (_, ct) =>
            {
                cts.Cancel();
                ct.ThrowIfCancellationRequested();
                await Task.CompletedTask;
            }, cancellationToken: cts.Token));

        // Root retry span gets the OCE
        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal("System.OperationCanceledException", executeActivity.GetTagItem("error.type"));

        // The single ydb.Try (first attempt, no backoff attr) also gets the OCE
        var tryActivity =
            GetSingleActivity(activities, "ydb.Try", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Null(tryActivity.GetTagItem("ydb.retry.backoff_ms"));
        Assert.Equal("System.OperationCanceledException", tryActivity.GetTagItem("error.type"));
    }

    [Fact]
    public async Task ExecuteInTransaction_CancellationAfterQueryExecuted_SetsErrorOnExecuteAndTrySpans()
    {
        await using var ydbDataSource = new YdbDataSource(ConnectionString);

        // Warm up the driver
        await using var initConnection = await ydbDataSource.OpenConnectionAsync();
        await new YdbCommand("SELECT 1;", initConnection).ExecuteNonQueryAsync();

        using var activityListener = StartListener(out var activities);
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            ydbDataSource.ExecuteInTransactionAsync(async (conn, ct) =>
            {
                await new YdbCommand("SELECT 1;", conn).ExecuteNonQueryAsync(ct);
                cts.Cancel();
                ct.ThrowIfCancellationRequested();
            }, cancellationToken: cts.Token));

        // Root retry span gets the OCE
        var executeActivity =
            GetSingleActivity(activities, "ydb.RunWithRetry", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Equal("System.OperationCanceledException", executeActivity.GetTagItem("error.type"));

        // Single ydb.Try (first attempt) gets the same OCE
        var tryActivity =
            GetSingleActivity(activities, "ydb.Try", expectedStatusCode: ActivityStatusCode.Error);
        Assert.Null(tryActivity.GetTagItem("ydb.retry.backoff_ms"));
        Assert.Equal("System.OperationCanceledException", tryActivity.GetTagItem("error.type"));
    }

    private static ActivityListener StartListener(out List<Activity> activities)
    {
        var captured = new List<Activity>();

        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Ydb.Sdk",
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
        ActivityStatusCode? expectedStatusCode = ActivityStatusCode.Unset,
        string? expectedStatusDescription = null
    )
    {
        var filtered = activities.Where(a => a.DisplayName == expectedDisplayName).ToList();
        Assert.Single(filtered);

        var activity = filtered[0];
        Assert.Equal(expectedDisplayName, activity.DisplayName);
        Assert.Equal(expectedStatusCode ?? ActivityStatusCode.Unset, activity.Status);
        if (expectedStatusDescription != null)
        {
            Assert.Equal(expectedStatusDescription, activity.StatusDescription);
        }

        return activity;
    }

    private static void AssertCommonDbTags(Activity activity)
    {
        Assert.Equal("ydb", activity.GetTagItem("db.system.name"));
        Assert.Equal(ConnectionSettings.Database, activity.GetTagItem("db.namespace"));
        Assert.Equal(ConnectionSettings.Host, activity.GetTagItem("server.address"));
        Assert.Equal(ConnectionSettings.Port.ToString(), activity.GetTagItem("server.port")?.ToString());
    }
}
