using System.Diagnostics;
using System.Reflection;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tracing;

internal static class YdbActivitySource
{
    private static readonly ActivitySource Instance = new("Ydb.Sdk", LibraryVersion);

    internal static Activity? StartActivity(string spanName, ActivityKind activityKind = ActivityKind.Client) =>
        Instance.StartActivity(spanName, activityKind);

    internal static void SetException(this Activity activity, Exception exception)
    {
        if (exception is YdbException ydbException)
        {
            activity.SetTag("db.response.status_code", ydbException.Code);
            activity.SetTag("error.type", ydbException.Code);
        }
        else
        {
            activity.SetTag("error.type", exception.GetType().FullName);
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
    }

    internal static void SetRetryAttributes(this Activity activity, TimeSpan retryInterval, int attempt)
    {
        if (!activity.IsAllDataRequested) return;
        activity.SetTag("ydb.retry.attempt", attempt);
        activity.SetTag("ydb.retry.backoff_ms", retryInterval.TotalMilliseconds);
    }

    private static string LibraryVersion => typeof(YdbActivitySource).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
        .InformationalVersion ?? "UNKNOWN";
}
