using System.Collections.Concurrent;
using Ydb.Coordination;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Watcher;

public class WatcherRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, WatchSubscription> _watchesByName = new();
    private readonly ConcurrentDictionary<ulong, WatchSubscription> _watchesByReqId = new();
    private volatile bool _isDisposed;

    public WatchSubscription Watch(string name)
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(WatcherRegistry));
        var sub = new WatchSubscription();

        _watchesByName.AddOrUpdate(
            name,
            _ => sub,
            (_, old) =>
            {
                old.Dispose();
                return sub;
            });

        return sub;
    }

    public void RemapWatch(string name, WatchSubscription subscription, ulong reqId)
    {
        if (_isDisposed)
            return;
        if (!_watchesByName.TryGetValue(name, out var active))
            return;

        if (!ReferenceEquals(active, subscription))
            return;

        if (subscription.ReqId != 0L)
        {
            _watchesByReqId.TryRemove(subscription.ReqId, out _);
        }

        subscription.ReqId = reqId;

        _watchesByReqId[reqId] = subscription;
    }

    public void RemoveWatch(string name, WatchSubscription subscription)
    {
        if (_isDisposed)
            return;
        if (_watchesByName.TryGetValue(name, out var active) &&
            ReferenceEquals(active, subscription))
        {
            _watchesByName.TryRemove(name, out _);
        }

        if (subscription.ReqId != 0)
        {
            _watchesByReqId.TryRemove(subscription.ReqId, out _);
        }
    }

    public void Notify(SessionResponse.Types.DescribeSemaphoreChanged evt)
    {
        if (_isDisposed)
            return;
        if (_watchesByReqId.TryGetValue(evt.ReqId, out var subscription))
        {
            subscription.Push(new SemaphoreChangedEvent(evt));
        }
    }

    public void NotifyAllWatches()
    {
        if (_isDisposed)
            return;
        foreach (var subscription in _watchesByName.Values)
        {
            subscription.Push(new SemaphoreChangedEvent());
        }
    }

    public void Dispose()
    {
        if (_isDisposed)
            return;

        _isDisposed = true;

        var subscriptions = _watchesByName.Values.ToArray();

        _watchesByName.Clear();
        _watchesByReqId.Clear();
        try
        {
            foreach (var sub in subscriptions)
            {
                sub.Dispose();
            }
        }
        finally
        {
            GC.SuppressFinalize(this);
        }
    }
}
