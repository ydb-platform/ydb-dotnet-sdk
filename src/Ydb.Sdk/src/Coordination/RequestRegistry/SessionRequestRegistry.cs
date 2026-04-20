using System.Collections.Concurrent;
using Ydb.Coordination;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination.RequestRegistry;

public class SessionRequestRegistry
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

    public PendingRequest Register(ulong reqId, SessionRequest request)
    {
        if (_closed)
            throw new InvalidOperationException("Session request registry is closed");

        var tcs = new TaskCompletionSource<SessionResponse>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        var pending = new PendingRequest(tcs, request);

        if (!_pending.TryAdd(reqId, pending))
            throw new InvalidOperationException($"Duplicate reqId: {reqId}");

        return pending;
    }

    public bool TryResolve(ulong reqId, Func<SessionResponse> resultFactory)
    {
        if (!_pending.TryRemove(reqId, out var pending))
            return false;

        try
        {
            var result = resultFactory();
            pending.Tcs.TrySetResult(result);
        }
        catch (Exception ex)
        {
            pending.Tcs.TrySetException(ex);
        }

        return true;
    }

    public bool TryCancel(ulong reqId, CancellationToken ct)
    {
        if (!_pending.TryRemove(reqId, out var pending))
            return false;

        pending.Tcs.TrySetCanceled(ct);
        return true;
    }

    public void Reconnect()
    {
        if (_closed)
            return;

        foreach (var (_, pending) in _pending)
        {
            pending.Tcs.TrySetException(new YdbException("Reconnect session"));
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
}
