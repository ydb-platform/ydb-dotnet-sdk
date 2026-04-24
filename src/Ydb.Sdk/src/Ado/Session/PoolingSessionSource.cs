using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Ydb.Query;

namespace Ydb.Sdk.Ado.Session;

internal sealed class PoolingSessionSource<T> : ISessionSource where T : PoolingSessionBase<T>
{
    private const int DisposeTimeoutSeconds = 10;

    private readonly ConcurrentStack<T> _idleSessions = new();
    private readonly ConcurrentQueue<TaskCompletionSource<T?>> _waiters = new();
    private readonly CancellationTokenSource _disposeCts = new();

    private readonly IPoolingSessionFactory<T> _sessionFactory;
    private readonly int _minPoolSize;
    private readonly int _maxPoolSize;
    private readonly T?[] _sessions;
    private readonly int _createSessionTimeout;
    private readonly TimeSpan _sessionIdleTimeout;
    private readonly Timer _cleanerTimer;
    private readonly ILogger _logger;

    private volatile int _numSessions;
    private volatile int _disposed;

    private bool IsDisposed => _disposed == 1;

    public PoolingSessionSource(IPoolingSessionFactory<T> sessionFactory, YdbConnectionStringBuilder settings)
    {
        _sessionFactory = sessionFactory;
        _minPoolSize = settings.MinPoolSize;
        _maxPoolSize = settings.MaxPoolSize;

        if (_minPoolSize > _maxPoolSize)
        {
            throw new ArgumentException(
                $"Connection can't have 'Max Session Pool' {_maxPoolSize} under 'Min Session Pool' {_minPoolSize}");
        }

        _sessions = new T?[_maxPoolSize];
        _createSessionTimeout = settings.CreateSessionTimeout;
        _sessionIdleTimeout = TimeSpan.FromSeconds(settings.SessionIdleTimeout);
        _cleanerTimer = new Timer(CleanIdleSessions, this, _sessionIdleTimeout, _sessionIdleTimeout);
        _logger = settings.LoggerFactory.CreateLogger<PoolingSessionSource<T>>();

        MetricsReporter = new YdbMetricsReporter(_maxPoolSize, _minPoolSize, Statistics, settings);
    }

    public YdbMetricsReporter MetricsReporter { get; }

    private (int Idle, int Busy) Statistics()
    {
        // The snapshot is inherently approximate in a concurrent system.
        // We count sessions in the In (idle) state by scanning the fixed-size _sessions array.
        // This avoids the negative-busy bug that occurs when CleanIdleSessions decrements
        // _numSessions while the session still sits in _idleSessions stack.
        var idle = 0;
        for (var i = 0; i < _maxPoolSize; i++)
        {
            var s = Volatile.Read(ref _sessions[i]);
            if (s is { State: PoolingSessionState.In })
                idle++;
        }

        var total = _numSessions;
        return (Math.Min(idle, total), total - Math.Min(idle, total));
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return TryGetIdleSession(out var session)
            ? new ValueTask<ISession>(session)
            : RentAsync(cancellationToken);
    }

    public IDriver Driver => _sessionFactory.Driver;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryGetIdleSession([NotNullWhen(true)] out T? session)
    {
        while (_idleSessions.TryPop(out session))
        {
            if (CheckIdleSession(session))
            {
                return true;
            }
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CheckIdleSession([NotNullWhen(true)] T? session)
    {
        if (session == null)
        {
            return false;
        }

        if (session.IsBroken)
        {
            CloseSession(session);

            return false;
        }

        return session.CompareAndSet(PoolingSessionState.In, PoolingSessionState.Out);
    }

    private async ValueTask<ISession> RentAsync(CancellationToken cancellationToken)
    {
        using var ctsGetSession = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (_createSessionTimeout > 0)
            ctsGetSession.CancelAfter(TimeSpan.FromSeconds(_createSessionTimeout));

        var finalToken = ctsGetSession.Token;

        var session = await OpenNewSession(finalToken).ConfigureAwait(false);
        if (session != null)
            return session;

        MetricsReporter.ReportPendingConnectionRequestStart();

        while (true)
        {
            // Statement order is important
            var waiterTcs = new TaskCompletionSource<T?>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Enqueue(waiterTcs);
            if (_idleSessions.TryPop(out session))
            {
                if (!waiterTcs.TrySetResult(null))
                {
                    if (waiterTcs.Task is { IsCompleted: true, Result: not null } t)
                    {
                        _idleSessions.Push(t.Result);
                    }

                    WakeUpWaiter();
                }

                if (CheckIdleSession(session))
                    return session;

                session = await OpenNewSession(finalToken).ConfigureAwait(false);
                if (session != null)
                    return session;

                continue;
            }

            await using var _ = finalToken.Register(() =>
                {
                    MetricsReporter.ReportConnectionTimeout();
                    waiterTcs.TrySetException(
                        new YdbException($"The connection pool has been exhausted, either raise 'MaxPoolSize' " +
                                         $"(currently {_maxPoolSize}) or 'CreateSessionTimeout' " +
                                         $"(currently {_createSessionTimeout} seconds) in your connection string."));
                }, useSynchronizationContext: false
            );
            await using var disposeRegistration = _disposeCts.Token.Register(
                () => waiterTcs.TrySetException(ObjectDisposedException),
                useSynchronizationContext: false
            );
            session = await waiterTcs.Task.ConfigureAwait(false);

            if (CheckIdleSession(session) || TryGetIdleSession(out session))
                return session;

            session = await OpenNewSession(finalToken).ConfigureAwait(false);
            if (session != null)
                return session;
        }
    }

    private async ValueTask<T?> OpenNewSession(CancellationToken cancellationToken)
    {
        for (var numSessions = _numSessions; numSessions < _maxPoolSize; numSessions = _numSessions)
        {
            if (Interlocked.CompareExchange(ref _numSessions, numSessions + 1, numSessions) != numSessions)
                continue;

            try
            {
                if (IsDisposed)
                    throw ObjectDisposedException;

                var session = _sessionFactory.NewSession(this);
                await session.Open(cancellationToken);

                for (var i = 0; i < _maxPoolSize; i++)
                {
                    if (Interlocked.CompareExchange(ref _sessions[i], session, null) == null)
                        return session;
                }

                throw new YdbException($"Could not find free slot in {_sessions} when opening. Please report a bug.");
            }
            catch
            {
                Interlocked.Decrement(ref _numSessions);

                WakeUpWaiter();

                throw;
            }
        }

        return null;
    }

    private void WakeUpWaiter()
    {
        while (_waiters.TryDequeue(out var waiter) && waiter.TrySetResult(null))
        {
        } // wake up waiter!
    }

    public void Return(T session)
    {
        if (session.IsBroken || IsDisposed)
        {
            CloseSession(session);

            return;
        }

        // Statement order is important
        session.IdleStartTime = DateTime.UtcNow;
        session.Set(PoolingSessionState.In);

        while (_waiters.TryDequeue(out var waiter))
        {
            if (waiter.TrySetResult(session))
            {
                return;
            }
        }

        _idleSessions.Push(session);

        WakeUpWaiter();
    }

    private void CloseSession(T session)
    {
        var i = 0;
        for (; i < _maxPoolSize; i++)
            if (Interlocked.CompareExchange(ref _sessions[i], null, session) == session)
                break;

        if (i == _maxPoolSize)
            return;

        _ = session.DeleteSession();

        Interlocked.Decrement(ref _numSessions);

        // If a session has been closed for any reason, we write a null to the idle sessions to wake up
        // a waiter, who will open a new session.
        WakeUpWaiter();
    }

    private static void CleanIdleSessions(object? state)
    {
        var pool = (PoolingSessionSource<T>)state!;
        var now = DateTime.UtcNow;

        for (var i = 0; i < pool._maxPoolSize; i++)
        {
            var session = Volatile.Read(ref pool._sessions[i]);

            if (
                session != null &&
                pool._numSessions > pool._minPoolSize &&
                session.IdleStartTime + pool._sessionIdleTimeout <= now &&
                session.CompareAndSet(PoolingSessionState.In, PoolingSessionState.Clean)
            )
            {
                pool.CloseSession(session);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
        {
            return;
        }

        await _cleanerTimer.DisposeAsync();
        await _disposeCts.CancelAsync();
        MetricsReporter.Dispose();

        var sw = Stopwatch.StartNew();
        var spinWait = new SpinWait();
        do
        {
            for (var i = 0; i < _maxPoolSize; i++)
            {
                var session = Volatile.Read(ref _sessions[i]);

                if (session != null && session.CompareAndSet(PoolingSessionState.In, PoolingSessionState.Clean))
                {
                    CloseSession(session);
                }
            }

            spinWait.SpinOnce();
        } while (_numSessions > 0 && sw.Elapsed < TimeSpan.FromSeconds(DisposeTimeoutSeconds));

        try
        {
            await _sessionFactory.DisposeAsync();
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to dispose the transport driver");
        }

        for (var i = 0; i < _maxPoolSize; i++)
        {
            var session = Volatile.Read(ref _sessions[i]);

            if (session == null || session.CompareAndSet(PoolingSessionState.Clean, PoolingSessionState.Clean))
                continue;
            _logger.LogCritical("Disposal timed out: Some sessions are still active");

            throw new YdbException("Timeout while disposing of the pool: some sessions are still active. " +
                                   "This may indicate a connection leak or suspended operations.");
        }
    }

    private static Exception ObjectDisposedException =>
        new ObjectDisposedException(nameof(PoolingSessionSource<T>), "The session source has been closed.");
}

internal interface IPoolingSessionFactory<T> : IAsyncDisposable where T : PoolingSessionBase<T>
{
    T NewSession(PoolingSessionSource<T> source);

    IDriver Driver { get; }
}

internal enum PoolingSessionState
{
    In,
    Out,
    Clean
}

internal abstract class PoolingSessionBase<T>(PoolingSessionSource<T> source) : ISession
    where T : PoolingSessionBase<T>
{
    protected YdbMetricsReporter MetricsReporter => source.MetricsReporter;

    private int _state = (int)PoolingSessionState.Out;

    internal PoolingSessionState State => (PoolingSessionState)Volatile.Read(ref _state);

    internal bool CompareAndSet(PoolingSessionState expected, PoolingSessionState actual) =>
        Interlocked.CompareExchange(ref _state, (int)actual, (int)expected) == (int)expected;

    internal void Set(PoolingSessionState state) => Interlocked.Exchange(ref _state, (int)state);

    internal DateTime IdleStartTime { get; set; }

    public abstract IDriver Driver { get; }

    public abstract bool IsBroken { get; }

    internal abstract Task Open(CancellationToken cancellationToken);

    internal abstract Task DeleteSession();

    public abstract ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    );

    public abstract Task CommitTransaction(string txId, Activity? dbActivity, CancellationToken cancellationToken);

    public abstract Task RollbackTransaction(string txId, Activity? dbActivity, CancellationToken cancellationToken);

    public abstract void OnNotSuccessStatusCode(StatusCode code);

    public void Dispose() => source.Return((T)this);
}
