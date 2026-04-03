using Ydb.Sdk.Coordination.Watcher;

namespace Ydb.Sdk.Coordination.Description;

public class SemaphoreWatcher : IDisposable
{
    private readonly SemaphoreDescription _description;
    private readonly WatchSubscription _subscription;

    public SemaphoreWatcher(SemaphoreDescription desc,WatchSubscription subscription)
    {
        _description = desc;
        _subscription = subscription;
    }

    public SemaphoreDescription GetDescription()
        => _description;
    
    public IAsyncEnumerable<SemaphoreChangedEvent> WatchAsync(CancellationToken ct = default)
        => _subscription.ReadAllAsync(ct);
    
    public void Dispose()
    {
        _subscription.Dispose();
    }
}
