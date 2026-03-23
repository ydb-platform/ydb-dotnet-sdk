namespace Ydb.Sdk.Coordination.Impl;

internal sealed class PendingRequest<TResult>
{
    public TaskCompletionSource<TResult> Tcs { get; }
    //public TRequest Request { get; }

    public PendingRequest(TaskCompletionSource<TResult> tcs)
    {
        Tcs = tcs;
        //Request = request;
    }
}
