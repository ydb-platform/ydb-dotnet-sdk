using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Internal;

/// <summary>
/// Tracks an in-flight request inside <see cref="CoordinationSession"/>.
/// </summary>
/// <remarks>
/// <para>
/// A request is "pinned" when its <c>reqId</c> must survive a stream reconnect — the prime
/// example is <c>AcquireSemaphore</c>, where the server keeps the waiter slot keyed by reqId.
/// The session worker resends pinned requests after each reconnect; non-pinned ones are
/// failed with <see cref="SessionReconnectException"/> so the caller can retry with a fresh reqId.
/// </para>
/// <para>
/// <see cref="ReqId"/> matches the value embedded in the outgoing <see cref="SessionRequest"/>.
/// </para>
/// </remarks>
internal sealed class PendingRequest
{
    public ulong ReqId { get; }
    public SessionRequest Request { get; set; }
    public bool IsPinned { get; }
    public TaskCompletionSource<SessionResponse> Tcs { get; }

    public PendingRequest(ulong reqId, SessionRequest request, bool isPinned)
    {
        ReqId = reqId;
        Request = request;
        IsPinned = isPinned;
        Tcs = new TaskCompletionSource<SessionResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
