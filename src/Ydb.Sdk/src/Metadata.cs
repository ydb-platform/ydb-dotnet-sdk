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

    // Incoming headers
    public const string RpcServerHintsHeader = "x-ydb-server-hints";

    //Incoming hints
    public const string GracefulShutdownHint = "session-close";
}
