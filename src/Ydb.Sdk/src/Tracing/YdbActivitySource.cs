using System.Diagnostics;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tracing;

internal static class YdbActivitySource
{
    private static readonly ActivitySource Instance = new("Ydb.Sdk", YdbSdkVersion.Value);

    internal static Activity? StartActivity(string spanName, ActivityKind activityKind = ActivityKind.Client) =>
        Instance.StartActivity(spanName, activityKind);

    internal static void SetException(this Activity activity, Exception exception)
    {
        if (!activity.IsAllDataRequested) return;

        if (exception is YdbException ydbException)
        {
            activity.SetTag("db.response.status_code", ydbException.Code);
            activity.SetTag("error.type", ydbException.Code
                is StatusCode.ClientTransportUnknown
                or StatusCode.ClientTransportUnavailable
                or StatusCode.ClientTransportTimeout
                or StatusCode.ClientTransportResourceExhausted
                or StatusCode.ClientTransportUnimplemented
                ? "transport_error"
                : "ydb_error");
        }
        else
        {
            activity.SetTag("error.type", exception.GetType().FullName ?? exception.GetType().Name);
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
    }

    internal static void SetRetryAttributes(this Activity activity, TimeSpan retryInterval)
    {
        if (!activity.IsAllDataRequested) return;
        activity.SetTag("ydb.retry.backoff_ms", retryInterval.TotalMilliseconds);
    }
}
