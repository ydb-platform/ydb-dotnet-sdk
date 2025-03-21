namespace Ydb.Sdk;

internal static class SocketHttpHandlerDefaults
{
    /// <summary>
    /// Default interval (in seconds) for sending keep-alive ping messages.
    /// </summary>
    internal const int DefaultKeepAlivePingSeconds = 10;

    /// <summary>
    /// Default timeout (in seconds) for receiving a response to a keep-alive ping.
    /// </summary>
    internal const int DefaultKeepAlivePingTimeoutSeconds = 10;
}
