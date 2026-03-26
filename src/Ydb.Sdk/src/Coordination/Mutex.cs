namespace Ydb.Sdk.Coordination;

public class Mutex
{
    private readonly Semaphore _semaphore;

    public Mutex(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }
}
