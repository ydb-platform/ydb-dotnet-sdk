namespace Ydb.Sdk.Coordination;

public class Mutex
{
    private readonly Semaphore _semaphore;

    public Mutex(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }
    
    /*
    public async Task<Lease> Acquire(ulong count, bool isEphemeral, byte[]? data,
        TimeSpan timeout)
    {

        return await _semaphore.Acquire();
    }
    */
}
