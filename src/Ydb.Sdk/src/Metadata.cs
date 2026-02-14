namespace Ydb.Sdk;

internal static class Metadata
{
    // Outgoing headers
    public const string RpcDatabaseHeader = "x-ydb-database";
    public const string RpcAuthHeader = "x-ydb-auth-ticket";
    public const string RpcRequestTypeHeader = "x-ydb-request-type";
    public const string RpcTraceIdHeader = "x-ydb-trace-id";
    public const string RpcSdkInfoHeader = "x-ydb-sdk-build-info";
    public const string RpcClientPid = "x-ydb-client-pid";

    // W3C trace-context propagation header (YDB supports "traceparent" on gRPC requests)
    public const string TraceParentHeader = "traceparent";

    // Incoming headers
    public const string RpcServerHintsHeader = "x-ydb-server-hints";
    public const string RpcClientCapabilitiesHeader = "x-ydb-client-capabilities";

    //Incoming hints
    public const string GracefulShutdownHint = "session-close";
}
