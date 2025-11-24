namespace Ydb.Sdk;

internal static class GrpcDefaultSettings
{
    /// <summary>
    /// Default interval (in seconds) for sending keep-alive ping messages.
    /// </summary>
    internal const int KeepAlivePingSeconds = 10;

    /// <summary>
    /// Default timeout (in seconds) for receiving a response to a keep-alive ping.
    /// </summary>
    internal const int KeepAlivePingTimeoutSeconds = 10;

    internal const int MaxSendMessageSize = 64 * 1024 * 1024; // 64 Mb

    internal const int MaxReceiveMessageSize = 64 * 1024 * 1024; // 64 Mb

    internal const int ConnectTimeoutSeconds = 5;

    internal const bool EnableMultipleHttp2Connections = false;

    internal const bool DisableDiscovery = false;

    /// <summary>
    /// Default idle timeout (in seconds) for pooled HTTP/2 connections.
    /// Set to prevent connection corruption from long-lived connections.
    /// </summary>
    internal const int PooledConnectionIdleTimeoutSeconds = 60;

    /// <summary>
    /// Default lifetime (in seconds) for pooled HTTP/2 connections.
    /// Set to prevent connection corruption from long-lived connections.
    /// </summary>
    internal const int PooledConnectionLifetimeSeconds = 300; // 5 minutes
}
