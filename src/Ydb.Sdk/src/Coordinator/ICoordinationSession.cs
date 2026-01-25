using Ydb.Sdk.Coordinator.Description;
using Ydb.Sdk.Coordinator.Settings;

namespace Ydb.Sdk.Coordinator;

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
        => state == State.Connected || state == State.Reconnected;

    // не понимаю почему предлгает Merge into logical pattern
    public static bool IsActive(State state)
        => state == State.Connected || state == State.Reconnecting || state == State.Reconnected;

    /// <summary>
    /// Establish new bidirectional gRPC stream.
    /// </summary>
    /// <returns>Task with status of operation.</returns>// исправить текст
    Task Connect();

    /// <summary>
    /// Send message to grpc server to stop stream. If server doesn't close connection,
    /// client cancels it itself.
    /// </summary>
    /// <returns>Task with status of operation.</returns>
    Task Stop();

    /// <summary>
    /// Current session identifier. If the connection wasn't established
    /// session will return -1.
    /// </summary>
    /// <returns>Active session identifier.</returns>
    long Id { get; }

    State CurrentState { get; }


    void AddStateListener(Action<State> listener);
    void RemoveStateListener(Action<State> listener);

    // исрпавить
    void IDisposable.Dispose()
    {
        // завершаем
    }

  

    /// <summary>
    /// Create a new semaphore. This operation doesn't change internal state of the coordination session
    /// so one session may be used for creating different semaphores.
    /// </summary>
    /// <param name="name">Name of the semaphore to create.</param>
    /// <param name="limit">Number of tokens that may be acquired by sessions.</param>
    /// <param name="data">User-defined data that will be attached to the semaphore.</param>
    /// <returns>
    /// Task with status of operation.
    /// If there already was a semaphore with such a name, you get ALREADY_EXISTS status.
    /// </returns>
    Task CreateSemaphore(string name, long limit, byte[]? data);

    /// <summary>
    /// Update data attached to the semaphore. This operation doesn't change internal state of the coordination session
    /// so one session may be used for updating different semaphores.
    /// </summary>
    /// <param name="name">Name of the semaphore to update.</param>
    /// <param name="data">User-defined data that will be attached to the semaphore.</param>
    /// <returns>Task with status of operation.</returns>
    Task UpdateSemaphore(string name, byte[]? data);


    /// <summary>
    /// Remove a semaphore. This operation doesn't change internal state of the coordination session
    /// so one session may be used for removing different semaphores.
    /// </summary>
    /// <param name="name">Name of the semaphore to remove.</param>
    /// <param name="force">Will delete semaphore even if it's currently acquired by sessions.</param>
    /// <returns>Task with status of operation.</returns>
    Task DeleteSemaphore(string name, bool force);

    /// <summary>
    /// Acquire a semaphore.
    /// WARNING: a single session can acquire only one semaphore at one moment.
    /// Later requests override previous operations with the same semaphore,
    /// e.g. to reduce acquired count, change timeout or attached data.
    /// </summary>
    /// <param name="name">Name of the semaphore to acquire.</param>
    /// <param name="count">Number of tokens to acquire on the semaphore.</param>
    /// <param name="data">User-defined binary data that may be attached to the operation.</param>
    /// <param name="timeout">
    /// Duration after which operation will fail if it's still waiting in the waiters queue.
    /// </param>
    /// <returns>
    /// If there is a semaphore with <c>name</c>, task will return a semaphore lease object.
    /// If there is no such a semaphore, task will complete exceptionally
    /// with appropriate error (analog of UnexpectedResultException).
    /// </returns>
    Task<ISemaphoreLease> AcquireSemaphore(
        string name,
        long count,
        byte[]? data,
        TimeSpan timeout);

    /// <summary>
    /// Acquire an ephemeral semaphore.
    /// Ephemeral semaphores are created with the first acquire operation and automatically deleted with
    /// the last release operation.
    /// WARNING: a single session can acquire only one semaphore at one moment.
    /// Later requests override previous operations with the same semaphore,
    /// e.g. to reduce acquired count, change timeout or attached data.
    /// </summary>
    /// <param name="name">Name of the semaphore to acquire.</param>
    /// <param name="exclusive">Flag of exclusive acquiring.</param>
    /// <param name="data">User-defined binary data that may be attached to the operation.</param>
    /// <param name="timeout">
    /// Duration after which operation will fail if it's still waiting in the waiters queue.
    /// </param>
    /// <returns>Task with a semaphore lease object.</returns>
    Task<ISemaphoreLease> AcquireEphemeralSemaphore(
        string name,
        bool exclusive,
        byte[]? data,
        TimeSpan timeout);

    Task<SemaphoreDescription> DescribeSemaphore(
        string name,
        DescribeSemaphoreMode mode);

    Task<SemaphoreWatcher> WatchSemaphore(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode);

    // ----------------------------- default methods -------------------------------

    /// <summary>
    /// Create a new semaphore without user data.
    /// </summary>
    /// <param name="name">Name of the semaphore to create.</param>
    /// <param name="limit">Number of tokens that may be acquired by sessions.</param>
    /// <returns>Task with status of operation.</returns>
    Task CreateSemaphore(string name, long limit)
        => CreateSemaphore(name, limit, null);

    /// <summary>
    /// Remove a semaphore without forcing.
    /// </summary>
    /// <param name="name">Name of the semaphore to remove.</param>
    /// <returns>Task with status of operation.</returns>
    Task DeleteSemaphore(string name)
        => DeleteSemaphore(name, false);

    /// <summary>
    /// Acquire a semaphore without user data.
    /// </summary>
    /// <param name="name">Name of the semaphore to acquire.</param>
    /// <param name="count">Number of tokens to acquire on the semaphore.</param>
    /// <param name="timeout">
    /// Duration after which operation will fail if it's still waiting in the waiters queue.
    /// </param>
    /// <returns>Task with a semaphore lease object.</returns>
    Task<ISemaphoreLease> AcquireSemaphore(
        string name,
        long count,
        TimeSpan timeout)
        => AcquireSemaphore(name, count, null, timeout);


    /// <summary>
    /// Acquire an ephemeral semaphore without user data.
    /// </summary>
    /// <param name="name">Name of the semaphore to acquire.</param>
    /// <param name="exclusive">Flag of exclusive acquiring.</param>
    /// <param name="timeout">
    /// Duration after which operation will fail if it's still waiting in the waiters queue.
    /// </param>
    /// <returns>Task with a semaphore lease object.</returns>
    Task<ISemaphoreLease> AcquireEphemeralSemaphore(
        string name,
        bool exclusive,
        TimeSpan timeout)
        => AcquireEphemeralSemaphore(name, exclusive, null, timeout);
}
