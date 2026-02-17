using System.Diagnostics;
using Xunit;
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

        var activity = GetSingleActivity(activities, "ydb.CreateSession");
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
        string? expectedOperationName = null,
        ActivityStatusCode? expectedStatusCode = ActivityStatusCode.Unset,
        string? expectedStatusDescription = null
    )
    {
        var filtered = activities.Where(a => a.DisplayName == expectedDisplayName).ToList();
        Assert.Single(filtered);

        var activity = filtered[0];
        Assert.Equal(expectedDisplayName, activity.DisplayName);
        Assert.Equal(expectedOperationName ?? expectedDisplayName, activity.OperationName);
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
