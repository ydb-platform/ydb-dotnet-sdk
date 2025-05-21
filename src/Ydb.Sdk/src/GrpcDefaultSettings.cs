namespace Ydb.Sdk;

internal static class GrpcDefaultSettings
{
    /// <summary>
    /// Default interval (in seconds) for sending keep-alive ping messages.
    /// </summary>
    internal const int DefaultKeepAlivePingSeconds = 10;

    /// <summary>
    /// Default timeout (in seconds) for receiving a response to a keep-alive ping.
    /// </summary>
    internal const int DefaultKeepAlivePingTimeoutSeconds = 10;

    internal const int MaxSendMessageSize = 64 * 1024 * 1024; // 64 Mb

    internal const int MaxReceiveMessageSize = 64 * 1024 * 1024; // 64 Mb
}
