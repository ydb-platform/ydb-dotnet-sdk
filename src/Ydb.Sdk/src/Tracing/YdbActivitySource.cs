using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tracing;

internal static class YdbActivitySource
{
    private static readonly ActivitySource Instance = new("Ydb.Sdk", LibraryVersion);

    internal static bool TryGetCurrent([NotNullWhen(true)] out Activity? activity)
    {
        var cur = Activity.Current;
        if (cur != null && ReferenceEquals(cur.Source, Instance))
        {
            activity = cur;
            return true;
        }

        activity = null;
        return false;
    }

    internal static Activity? StartActivity(string spanName) => Instance.StartActivity(spanName, ActivityKind.Client);

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

    private static string LibraryVersion => typeof(YdbActivitySource).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
        .InformationalVersion ?? "UNKNOWN";
}
