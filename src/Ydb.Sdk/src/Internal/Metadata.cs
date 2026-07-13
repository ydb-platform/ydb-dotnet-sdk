namespace Ydb.Sdk.Internal;

internal static class Metadata
{
    private const string RpcSdkInfoHeader = "x-ydb-sdk-build-info";
    
    // Outgoing headers
    internal const string RpcDatabaseHeader = "x-ydb-database";
    internal const string RpcAuthHeader = "x-ydb-auth-ticket";
    internal const string RpcRequestTypeHeader = "x-ydb-request-type";
    internal const string RpcTraceIdHeader = "x-ydb-trace-id";
    internal const string RpcClientPid = "x-ydb-client-pid";

    // W3C trace-context propagation header (YDB supports "traceparent" on gRPC requests)
    internal const string TraceParentHeader = "traceparent";

    // Incoming headers
    internal const string RpcServerHintsHeader = "x-ydb-server-hints";
    internal const string RpcClientCapabilitiesHeader = "x-ydb-client-capabilities";

    internal static readonly string AdoNetClientInfo = $"ado-net/{YdbSdkVersion.Value}";

    internal static void AddSdkBuildInfo(this Grpc.Core.Metadata metadata)
    {
        var sdkVersion = YdbSdkVersion.Value;
        var clientInfoChain = SdkClientInfoRegistry.Chain;
        var observabilityChain = ObservabilityInfo.BuildChain();
        
        var sdkBuildInfo = observabilityChain is null ? sdkVersion : $"{sdkVersion};{observabilityChain}";

        metadata.Add(RpcSdkInfoHeader, clientInfoChain is null ? sdkBuildInfo : $"{sdkBuildInfo};{clientInfoChain}");
    }
}
