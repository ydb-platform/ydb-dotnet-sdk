namespace Ydb.Sdk.Coordination;

public class Mutex
{
    private readonly Semaphore _semaphore;

    public Mutex(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }

    public async Task<Lease> Lock(CancellationToken cancellationToken)
        => await _semaphore.Acquire(ulong.MaxValue, true, null, null, cancellationToken);
}
