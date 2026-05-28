using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.RequestRegistry;

internal class PendingRequest
{
    public TaskCompletionSource<SessionResponse> Tcs { get; }
    public SessionRequest Request { get; }
    public bool IsPinned { get; }

    public PendingRequest(TaskCompletionSource<SessionResponse> tcs, SessionRequest request, bool isPinned)
    {
        Tcs = tcs;
        Request = request;
        IsPinned = isPinned;
    }
}
