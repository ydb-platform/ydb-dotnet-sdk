using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Dto;

internal sealed class PendingRequest<TResult>
{
    public TaskCompletionSource<TResult> Tcs { get; }
    public SessionRequest Request { get; }

    public PendingRequest(TaskCompletionSource<TResult> tcs, SessionRequest request)
    {
        Tcs = tcs;
        Request = request;
    }
}
