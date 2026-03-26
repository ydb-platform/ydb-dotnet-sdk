namespace Ydb.Sdk.Coordination;

public class Election
{
    private readonly Semaphore _semaphore;

    public Election(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }
}
