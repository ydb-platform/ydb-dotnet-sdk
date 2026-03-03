namespace Ydb.Sdk.Coordinator.Impl;

internal sealed class PendingRequest<TResult, TRequest>
{
    public TaskCompletionSource<TResult?> Tcs { get; }
    public TRequest Request { get; }

    public PendingRequest(TaskCompletionSource<TResult?> tcs, TRequest request)
    {
        Tcs = tcs;
        Request = request;
    }
}
