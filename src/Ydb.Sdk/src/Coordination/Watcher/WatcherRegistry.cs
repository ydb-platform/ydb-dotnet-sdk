using System.Collections.Concurrent;
using Ydb.Coordination;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Watcher;

public class WatcherRegistry
{
    private readonly ConcurrentDictionary<string, WatchSubscription> _watchesByName = new();
    private readonly ConcurrentDictionary<ulong, WatchSubscription> _watchesByReqId = new();

    public WatchSubscription Watch(string name)
    {
        if (_watchesByName.TryGetValue(name, out var previous))
        {
            previous.Dispose();
        }

        var subscription = new WatchSubscription(name);
        _watchesByName[name] = subscription;
        return subscription;
    }

    public void RemapWatch(string name, WatchSubscription subscription, ulong reqId)
    {
        var activeSubscription = _watchesByName.GetOrAdd(name, _ => new WatchSubscription(name));

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
            subscription.Push(new SemaphoreChangedEvent(false, false));// почистить мб надо
        }
    }
}
