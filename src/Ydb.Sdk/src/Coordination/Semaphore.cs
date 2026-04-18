using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using Ydb.Sdk.Coordination.Watcher;

namespace Ydb.Sdk.Coordination;

public class Semaphore
{
    public string Name { get; }
    private readonly SessionTransport _sessionTransport;

    public Semaphore(string name, SessionTransport sessionTransport)
    {
        Name = name;
        _sessionTransport = sessionTransport;
    }

    public ulong SessionId => _sessionTransport.SessionId;

    public async Task Create(ulong limit, byte[]? data, CancellationToken cancellationToken = default)
        => await _sessionTransport.CreateSemaphore(Name, limit, data, cancellationToken);


    public async Task Update(byte[]? data, CancellationToken cancellationToken = default)
        => await _sessionTransport.UpdateSemaphore(Name, data, cancellationToken);


    public async Task Delete(bool force, CancellationToken cancellationToken = default)
        => await _sessionTransport.DeleteSemaphore(Name, force, cancellationToken);


    public async Task<SemaphoreDescriptionClient> Describe(
        DescribeSemaphoreMode mode, CancellationToken cancellationToken = default)
        => await _sessionTransport.DescribeSemaphore(Name, mode, cancellationToken);


    public async Task<Lease> Acquire(ulong count, bool isEphemeral, byte[]? data,
        TimeSpan? timeout, CancellationToken cancellationToken = default)
    {
        await _sessionTransport.AcquireSemaphore(Name, count, isEphemeral, data, timeout, cancellationToken);
        return new Lease(this);
    }

    public async Task Release(CancellationToken cancellationToken = default)
        => await _sessionTransport.ReleaseSemaphore(Name, cancellationToken);

    public Task<WatchResult<SemaphoreDescriptionClient>> WatchSemaphore(DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode, CancellationToken cancellationToken = default)
        => _sessionTransport.WatchSemaphore(Name, describeMode, watchMode, cancellationToken);
}
