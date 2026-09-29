using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ydb.Sdk.Topic;

internal static class TopicMetricsUtils
{
    internal static void ReportSessionError(
        Counter<long> sessionErrors,
        KeyValuePair<string, object?>[] commonTags,
        StatusCode statusCode,
        bool retry)
    {
        if (!sessionErrors.Enabled)
        {
            return;
        }

        sessionErrors.Add(1, new TagList(commonTags)
        {
            { "retry_decision", retry ? "retry" : "stop" },
            { "status_code", statusCode.ToString() },
            {
                "error.type", statusCode switch
                {
                    StatusCode.Unspecified => "session_closed",
                    _ when statusCode.IsTransportError() => "transport_error",
                    _ => "ydb_error"
                }
            }
        });
    }
}
