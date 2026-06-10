using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Internal;

internal sealed record PendingRequest(ulong ReqId, SessionRequest Request, bool IsPinned)
{
    public TaskCompletionSource<SessionResponse> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
