namespace Ydb.Sdk.Internal;

internal static class MetadataSdkBuildInfoExtensions
{
    /// <summary>
    /// Appends the observability adoption chain (<c>ydb-sdk-tracing</c>/<c>ydb-sdk-metrics</c>)
    /// to the existing <c>x-ydb-sdk-build-info</c> header. Used only by Driver Discovery.
    /// </summary>
    internal static void AppendObservabilityChain(this Grpc.Core.Metadata metadata)
    {
        var observabilityChain = ObservabilityInfo.BuildChain();
        if (observabilityChain is null)
        {
            return;
        }

        var entry = metadata.Get(Metadata.RpcSdkInfoHeader);
        if (entry is null)
        {
            return;
        }

        metadata.Remove(entry);
        metadata.Add(Metadata.RpcSdkInfoHeader, $"{entry.Value};{observabilityChain}");
    }
}
