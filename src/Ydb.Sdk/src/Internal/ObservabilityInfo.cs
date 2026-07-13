using Ydb.Sdk.Ado;
using Ydb.Sdk.Tracing;

namespace Ydb.Sdk.Internal;

internal static class ObservabilityInfo
{
    internal const string TracingChainVersion = "0.1.0";
    internal const string MetricsChainVersion = "0.1.0";

    private const string TracingChainName = "ydb-sdk-tracing";
    private const string MetricsChainName = "ydb-sdk-metrics";

    internal static string? BuildChain()
    {
        var hasTracing = YdbActivitySource.HasListeners;
        var hasMetrics = YdbMetricsReporter.HasEnabledInstruments;

        if (!hasTracing && !hasMetrics)
        {
            return null;
        }

        if (!hasMetrics)
        {
            return $"{TracingChainName}/{TracingChainVersion}";
        }

        if (!hasTracing)
        {
            return $"{MetricsChainName}/{MetricsChainVersion}";
        }

        return $"{TracingChainName}/{TracingChainVersion};{MetricsChainName}/{MetricsChainVersion}";
    }
}
