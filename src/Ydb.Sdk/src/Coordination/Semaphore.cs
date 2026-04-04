using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using SemaphoreDescription = Ydb.Coordination.SemaphoreDescription;

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

    public ulong SessionId => _sessionRuntime.SessionId;

    public async Task Create(ulong limit, byte[]? data)
        => await _sessionRuntime.CreateSemaphore(Name, limit, data);


    public async Task Update(byte[]? data)
        => await _sessionRuntime.UpdateSemaphore(Name, data);


    public async Task Delete(bool force)
        => await _sessionRuntime.DeleteSemaphore(Name, force);


    public async Task<SemaphoreDescription> Describe(
        DescribeSemaphoreMode mode)
        => await _sessionRuntime.DescribeSemaphore(Name, mode);


    public async Task<Lease> Acquire(ulong count, bool isEphemeral, byte[]? data,
        TimeSpan timeout)
    {
        await _sessionRuntime.AcquireSemaphore(Name, count, isEphemeral, data, timeout);
        return new Lease(this);
    }

    public async Task Release()
        => await _sessionRuntime.ReleaseSemaphore(Name);

    public IAsyncEnumerable<Ydb.Sdk.Coordination.Description.SemaphoreDescription> WatchSemaphore(DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode)
        =>  _sessionRuntime.WatchSemaphore(Name, describeMode, watchMode);
}
