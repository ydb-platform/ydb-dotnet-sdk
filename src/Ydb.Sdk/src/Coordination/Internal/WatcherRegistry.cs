using System.Collections.Concurrent;
using Ydb.Coordination;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Internal;

/// <summary>
/// Maps semaphore names and server-side reqIds to <see cref="WatchSubscription"/> instances.
/// </summary>
/// <remarks>
/// <para>The session reader uses <see cref="Notify"/> to deliver per-watch changes; on every reconnect
/// the worker calls <see cref="NotifyAllWatches"/> so consumers re-issue the <c>DescribeSemaphore</c>
/// with a fresh reqId and re-arm the watch.</para>
/// </remarks>
internal sealed class WatcherRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, WatchSubscription> _byName = new();
    private readonly ConcurrentDictionary<ulong, WatchSubscription> _byReqId = new();

    public WatchSubscription Watch(string name)
    {
        var subscription = new WatchSubscription(name);

        _byName.AddOrUpdate(name, _ => subscription, (_, prev) =>
        {
            prev.Dispose();
            return subscription;
        });

        return subscription;
    }

    public void Bind(WatchSubscription subscription, ulong reqId)
    {
        if (!_byName.TryGetValue(subscription.Name, out var active) || !ReferenceEquals(active, subscription))
            return;

        if (subscription.ReqId != 0)
            _byReqId.TryRemove(subscription.ReqId, out _);

        subscription.ReqId = reqId;
        _byReqId[reqId] = subscription;
    }

    public void Remove(WatchSubscription subscription)
    {
        if (_byName.TryGetValue(subscription.Name, out var active) && ReferenceEquals(active, subscription))
            _byName.TryRemove(subscription.Name, out _);

        if (subscription.ReqId != 0)
            _byReqId.TryRemove(subscription.ReqId, out _);

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
