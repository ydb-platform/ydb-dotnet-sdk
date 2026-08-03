using Ydb.Sdk.Ado;
using Ydb.Sdk.Tracing;

namespace Ydb.Sdk.Internal;

internal static class MetadataSdkBuildInfoExtensions
{
    private const string TracingChain = ";ydb-sdk-tracing/0.1.0";
    private const string MetricsChain = ";ydb-sdk-metrics/0.1.0";

    /// <summary>
    /// Appends the observability adoption chain (<c>ydb-sdk-tracing</c>/<c>ydb-sdk-metrics</c>)
    /// to the existing <c>x-ydb-sdk-build-info</c> header. Used only by Driver Discovery.
    /// </summary>
    internal static void AppendObservabilityChain(this Grpc.Core.Metadata metadata)
    {
        // GetCallMetadata always sets x-ydb-sdk-build-info before discovery appends observability.
        var entry = metadata.Get(Metadata.RpcSdkInfoHeader)!;
        metadata.Remove(entry);
        metadata.Add(Metadata.RpcSdkInfoHeader,
            $"{entry.Value}{(YdbActivitySource.HasListeners ? TracingChain : string.Empty)}{(YdbMetricsReporter.HasEnabledInstruments ? MetricsChain : string.Empty)}");
    }
}
