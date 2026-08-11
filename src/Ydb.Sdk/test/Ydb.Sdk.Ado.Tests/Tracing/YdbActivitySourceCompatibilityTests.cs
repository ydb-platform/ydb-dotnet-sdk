using System.Diagnostics;
using Xunit;

namespace Ydb.Sdk.Ado.Tests.Tracing;

public class YdbActivitySourceCompatibilityTests
{
    [Fact]
    public void LegacyFacade_ForwardsToAdoActivitySource()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Ydb.Sdk",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        using var activity = global::Ydb.Sdk.Tracing.YdbActivitySource.StartActivity("legacy");

        Assert.NotNull(activity);
        Assert.Equal("Ydb.Sdk", activity.Source.Name);

        global::Ydb.Sdk.Tracing.YdbActivitySource.SetException(activity, new InvalidOperationException("boom"));
        global::Ydb.Sdk.Tracing.YdbActivitySource.SetRetryAttributes(activity, TimeSpan.FromMilliseconds(1));

        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal(1D, activity.GetTagItem("ydb.retry.backoff_ms"));
    }
}
