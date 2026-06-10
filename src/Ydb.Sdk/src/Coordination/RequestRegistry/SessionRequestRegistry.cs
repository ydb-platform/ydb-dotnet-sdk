using System.Collections.Concurrent;
using Ydb.Coordination;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination.RequestRegistry;

internal class SessionRequestRegistry
{
    private long _reqIdCounter;
    private volatile bool _closed;

    private readonly ConcurrentDictionary<ulong, PendingRequest> _pending = new();

    public ulong NextReqId()
    {
        if (_closed)
            throw new InvalidOperationException("Session request registry is closed");

        return (ulong)Interlocked.Increment(ref _reqIdCounter);
    }
    

    public bool Resolve(ulong reqId, SessionResponse response)
    {
        if (!_pending.TryRemove(reqId, out var pending))
            return false;
        pending.Tcs.TrySetResult(response);
        return true;
    }

    public bool TryCancel(ulong reqId, CancellationToken ct)
    {
        if (!_pending.TryRemove(reqId, out var pending))
            return false;
        pending.Tcs.TrySetCanceled(ct);
        return true;
    }

    public PendingRequest Register(ulong reqId, SessionRequest request)
        => Register(reqId, request, isPinned: false);

    public PendingRequest RegisterPinned(ulong reqId, SessionRequest request)
        => Register(reqId, request, isPinned: true);

    public void Reconnect()
    {
        if (_closed)
            return;

        foreach (var (_, pending) in _pending)
        {
            pending.Tcs.TrySetException(new SessionReconnectException(pending.IsPinned));
        }

        _pending.Clear();
    }

    public void Close()
    {
        if (_closed)
            return;

        _closed = true;

        foreach (var (_, pending) in _pending)
        {
            pending.Tcs.TrySetException(new YdbException("Session closed"));
        }

        _pending.Clear();
    }


    public void Dispose() => Close();
    
    private PendingRequest Register(ulong reqId, SessionRequest request, bool isPinned)
    {
        if (_closed)
            throw new InvalidOperationException("Session request registry is closed");

        var tcs = new TaskCompletionSource<SessionResponse>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var pending = new PendingRequest(tcs, request, isPinned);

        if (!_pending.TryAdd(reqId, pending))
            throw new InvalidOperationException($"Duplicate reqId: {reqId}");

        return pending;
    }
}
