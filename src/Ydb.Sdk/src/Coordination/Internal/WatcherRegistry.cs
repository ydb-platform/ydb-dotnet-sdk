using System.Collections.Concurrent;
using Ydb.Coordination;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Internal;

internal sealed class WatcherRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, WatchSubscription> _byName = new();
    private readonly ConcurrentDictionary<ulong, WatchSubscription> _byReqId = new();

    /// <summary>
    /// Registers a new watch for <paramref name="name"/>. Only one watcher per name is supported
    /// per session — a concurrent or subsequent call for the same name throws.
    /// </summary>
    public WatchSubscription Watch(string name)
    {
        var subscription = new WatchSubscription(name);
        if (_byName.TryAdd(name, subscription))
            return subscription;

        subscription.Dispose();
        throw new InvalidOperationException(
            $"A watcher for semaphore '{name}' is already registered on this session");
    }

    public void Bind(WatchSubscription subscription, ulong reqId)
    {
        if (!_byName.TryGetValue(subscription.Name, out var active) || !ReferenceEquals(active, subscription))
            return;

        var previousReqId = subscription.ReqId;
        if (previousReqId != 0)
            _byReqId.TryRemove(new KeyValuePair<ulong, WatchSubscription>(previousReqId, subscription));

        subscription.ReqId = reqId;
        _byReqId[reqId] = subscription;
    }

    public void Remove(WatchSubscription subscription)
    {
        _byName.TryRemove(new KeyValuePair<string, WatchSubscription>(subscription.Name, subscription));

        if (subscription.ReqId != 0)
            _byReqId.TryRemove(new KeyValuePair<ulong, WatchSubscription>(subscription.ReqId, subscription));

        subscription.Dispose();
    }

    public void Notify(SessionResponse.Types.DescribeSemaphoreChanged change)
    {
        if (_byReqId.TryGetValue(change.ReqId, out var subscription))
            subscription.Push(new SemaphoreChangedEvent(change));
    }

    public void NotifyAllWatches()
    {
        foreach (var subscription in _byName.Values)
            subscription.Push(new SemaphoreChangedEvent());
    }

    public void Dispose()
    {
        var subscriptions = _byName.Values.ToArray();
        _byName.Clear();
        _byReqId.Clear();

        foreach (var subscription in subscriptions)
            subscription.Dispose();
    }
}
