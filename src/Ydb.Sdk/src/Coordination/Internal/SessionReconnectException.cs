namespace Ydb.Sdk.Coordination.Internal;

/// <summary>
/// Thrown into a non-pinned <see cref="PendingRequest.Tcs"/> when the underlying gRPC stream
/// is lost. Tells the public Send* helper to allocate a fresh reqId and retry.
/// </summary>
internal sealed class SessionReconnectException : Exception
{
    public SessionReconnectException()
        : base("Coordination request must be re-sent after stream reconnect")
    {
    }
}
