namespace Ydb.Sdk.Coordination;

// BAD LEASE
public class Lease
{
    private readonly Semaphore _semaphore;

    public Lease(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }

    public string GetSemaphoreName()
        => _semaphore.Name;

    public async Task ReleaseAsync()
        => await _semaphore.Release();
}
