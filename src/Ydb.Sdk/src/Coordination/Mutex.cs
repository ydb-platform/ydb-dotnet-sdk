namespace Ydb.Sdk.Coordination;

public class Mutex
{
    public string Name { get; }
    private readonly SessionTransport _sessionTransport;

    public Mutex(string name, SessionTransport sessionTransport)
    {
        Name = name;
        _sessionTransport = sessionTransport;
    }

    public async Task<Lease> Lock(CancellationToken cancellationToken)
        => await _sessionTransport.AcquireSemaphore(Name, ulong.MaxValue, true, null, null, cancellationToken);

    public async Task<Lease?> TryLock(CancellationToken cancellationToken)
        => await _sessionTransport.TryAcquireSemaphore(Name, ulong.MaxValue, true, null, cancellationToken);
}
