namespace Ydb.Sdk.Coordination.RequestRegistry;

internal sealed class SessionReconnectException : Exception
{
    public SessionReconnectException(bool isPinned)
        : base(isPinned
            ? "Pinned coordination request must be re-sent after reconnect"
            : "Coordination request must be re-sent after reconnect")
    {
        IsPinned = isPinned;
    }

    public bool IsPinned { get; }
}
