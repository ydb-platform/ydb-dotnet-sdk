using Ydb.Coordination;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class Semaphore
{
    public string Name { get; }
    private readonly SessionRuntime _sessionRuntime;

    public Semaphore(string name, SessionRuntime sessionRuntime)
    {
        Name = name;
        _sessionRuntime = sessionRuntime;
    }


    public async Task Create(ulong limit, byte[]? data)
        => await _sessionRuntime.CreateSemaphore(Name, limit, data);


    public async Task Update(byte[]? data)
        => await _sessionRuntime.UpdateSemaphore(Name, data);


    public async Task Delete(bool force)
        => await _sessionRuntime.DeleteSemaphore(Name, force);


    public async Task<SessionResponse.Types.DescribeSemaphoreResult> Describe(
        DescribeSemaphoreMode mode)
        => await _sessionRuntime.DescribeSemaphore(Name, mode);


    public async Task<Lease> Acquire(ulong count, bool ephemeral, byte[]? data,
        TimeSpan timeout)
    {
        await _sessionRuntime.AcquireSemaphore(Name, count, ephemeral, data, timeout);
        return new Lease(this);
    }

    public async Task Release()
        => await _sessionRuntime.ReleaseSemaphore(Name);
}
