using Ydb.Coordination;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class Semaphore
{
    private readonly string _name;
    private readonly CoordinationSession _coordinationSession;

    public Semaphore(string name, CoordinationSession session)
    {
        _name = name;
        _coordinationSession = session;
    }

    public string Name => _name;

    public async Task Create(ulong limit, byte[]? data)
        => await _coordinationSession.CreateSemaphore(_name, limit, data);


    public async Task Update(byte[]? data)
        => await _coordinationSession.UpdateSemaphore(_name, data);


    public async Task Delete(bool force)
        => await _coordinationSession.DeleteSemaphore(_name, force);


    public async Task<SessionResponse.Types.DescribeSemaphoreResult> Describe(string name,
        DescribeSemaphoreMode mode)
        => await _coordinationSession.DescribeSemaphore(_name, mode);


    public async Task<Lease> Acquire(ulong count, bool ephemeral, byte[]? data,
        TimeSpan timeout)
    {
        await _coordinationSession.AcquireSemaphore(_name, count, ephemeral, data, timeout);
        return new Lease(this);
    }

    public async Task Release()
        => await _coordinationSession.ReleaseSemaphore(_name);
}
