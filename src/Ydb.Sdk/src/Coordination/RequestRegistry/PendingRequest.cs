using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.RequestRegistry;

public class PendingRequest
{
    public TaskCompletionSource<SessionResponse> Tcs { get; }
    public SessionRequest Request { get; }

    public PendingRequest(TaskCompletionSource<SessionResponse> tcs, SessionRequest request)
    {
        Tcs = tcs;
        Request = request;
    }
}
