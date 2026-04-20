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
        var subscription = new WatchSubscription();

        _watchesByName.AddOrUpdate(name, _ => subscription,
            (_, oldSubscription) =>
            {
                oldSubscription.Dispose();
                return subscription;
            });

        return subscription;
    }

    public void RemapWatch(string name, WatchSubscription subscription, ulong reqId)
    {
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
        if (_watchesByReqId.TryGetValue(evt.ReqId, out var subscription))
        {
            subscription.Push(new SemaphoreChangedEvent(evt));
        }
    }

    public void NotifyAllWatches()
    {
        foreach (var subscription in _watchesByName.Values)
        {
            subscription.Push(new SemaphoreChangedEvent());
        }
    }

    public void Dispose()
    {
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
