using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public interface ICoordinationSession : IDisposable
{
    public enum State
    {
        Initial,

        Connecting,

        Connected,

        Reconnecting,

        Reconnected,

        Closed,

        Lost
    }

    public static bool IsConnected(State state)
        => (state == State.Connected) | (state == State.Reconnected);

    public static bool IsActive(State state)
        => (state == State.Connected) | (state == State.Reconnecting) | (state == State.Reconnected);


    Task Connect();


    Task Stop();


    long Id { get; }

    State CurrentState { get; }


    void AddStateListener(Action<State> listener);
    void RemoveStateListener(Action<State> listener);

    // исрпавить
    void IDisposable.Dispose()
    {
        // завершаем
    }


    Task CreateSemaphore(string name, long limit, byte[]? data);


    Task UpdateSemaphore(string name, byte[]? data);


    Task DeleteSemaphore(string name, bool force);


    Task<ISemaphoreLease> AcquireSemaphore(
        string name,
        long count,
        byte[]? data,
        TimeSpan timeout);

    Task<ISemaphoreLease> AcquireEphemeralSemaphore(
        string name,
        bool exclusive,
        byte[]? data,
        TimeSpan timeout);

    Task<SemaphoreDescriptionClient> DescribeSemaphore(
        string name,
        DescribeSemaphoreMode mode);

    Task<SemaphoreWatcher> WatchSemaphore(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode);

    // ----------------------------- default methods -------------------------------


    Task CreateSemaphore(string name, long limit)
        => CreateSemaphore(name, limit, null);


    Task DeleteSemaphore(string name)
        => DeleteSemaphore(name, false);


    Task<ISemaphoreLease> AcquireSemaphore(
        string name,
        long count,
        TimeSpan timeout)
        => AcquireSemaphore(name, count, null, timeout);


    Task<ISemaphoreLease> AcquireEphemeralSemaphore(
        string name,
        bool exclusive,
        TimeSpan timeout)
        => AcquireEphemeralSemaphore(name, exclusive, null, timeout);
}
