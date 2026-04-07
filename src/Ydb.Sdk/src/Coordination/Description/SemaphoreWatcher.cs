using Ydb.Sdk.Coordination.Watcher;

namespace Ydb.Sdk.Coordination.Description;

public class SemaphoreWatcher : IDisposable
{
    private readonly SemaphoreDescriptionClient _descriptionClient;
    private readonly WatchSubscription _subscription;

    public SemaphoreWatcher(SemaphoreDescriptionClient desc, WatchSubscription subscription)
    {
        _descriptionClient = desc;
        _subscription = subscription;
    }

    public SemaphoreDescriptionClient GetDescription()
        => _descriptionClient;

    public IAsyncEnumerable<SemaphoreChangedEvent> WatchAsync(CancellationToken ct = default)
        => _subscription.ReadAllAsync(ct);

    public void Dispose()
        => _subscription.Dispose();
}
