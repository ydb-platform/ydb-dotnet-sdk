namespace Ydb.Sdk.Coordinator.Description;

public class SemaphoreWatcher
{
    private readonly SemaphoreDescription _description;
    private readonly Task<SemaphoreChangedEvent> _changedFuture;

    public SemaphoreWatcher(SemaphoreDescription desc, Task<SemaphoreChangedEvent> changed)
    {
        _description = desc;
        _changedFuture = changed;
    }

    public SemaphoreDescription GetDescription()
        => _description;

    public Task<SemaphoreChangedEvent> GetChangedFuture()
        => _changedFuture;
}
