using System.Diagnostics;

namespace Ydb.Sdk.Tracing;

// Kept for binary compatibility with already published EntityFrameworkCore.Ydb versions.
internal static class YdbActivitySource
{
    internal static bool HasListeners => Ado.Tracing.YdbActivitySource.HasListeners;

    internal static Activity? StartActivity(string spanName, ActivityKind activityKind = ActivityKind.Client) =>
        Ado.Tracing.YdbActivitySource.StartActivity(spanName, activityKind);

    internal static void SetException(this Activity activity, Exception exception) =>
        Ado.Tracing.YdbActivitySource.SetException(activity, exception);

    internal static void SetRetryAttributes(this Activity activity, TimeSpan retryInterval) =>
        Ado.Tracing.YdbActivitySource.SetRetryAttributes(activity, retryInterval);
}
