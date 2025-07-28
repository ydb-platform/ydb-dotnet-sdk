using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Session;

internal sealed class PoolingSessionSource<T> : ISessionSource where T : PoolingSessionBase<T>
{
    private readonly ConcurrentStack<T> _idleSessions = new();
    private readonly ConcurrentQueue<TaskCompletionSource<T?>> _waiters = new();

    private readonly IPoolingSessionFactory<T> _sessionFactory;
    private readonly int _minSessionSize;
    private readonly int _maxSessionSize;
    private readonly T?[] _sessions;
    private readonly int _createSessionTimeout;
    private readonly TimeSpan _sessionIdleTimeout;
    private readonly Timer _cleanerTimer;

    private volatile int _numSessions;
    private volatile int _waiterSize;

    public PoolingSessionSource(
        IPoolingSessionFactory<T> sessionFactory,
        YdbConnectionStringBuilder settings
    )
    {
        _sessionFactory = sessionFactory;
        _minSessionSize = settings.MinSessionPool;
        _maxSessionSize = settings.MaxSessionPool;

        if (_minSessionSize > _maxSessionSize)
        {
            throw new ArgumentException(
                $"Connection can't have 'Max Session Pool' {_maxSessionSize} under 'Min Session Pool' {_minSessionSize}");
        }

        _sessions = new T?[_maxSessionSize];
        _createSessionTimeout = settings.CreateSessionTimeout;
        _sessionIdleTimeout = TimeSpan.FromSeconds(settings.SessionIdleTimeout);
        _cleanerTimer = new Timer(CleanIdleSessions, this, _sessionIdleTimeout, _sessionIdleTimeout);
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken = default) =>
        TryGetIdleSession(out var session)
            ? new ValueTask<ISession>(session)
            : RentAsync(cancellationToken);

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
        if (session == null || session.State == PoolingSessionState.Clean)
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

        while (true)
        {
            // Statement order is important
            var waiterTcs = new TaskCompletionSource<T?>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Enqueue(waiterTcs);
            if (TryGetIdleSession(out session))
            {
                if (!waiterTcs.TrySetResult(null))
                {
                    var raceSession = await waiterTcs.Task;

                    if (CheckIdleSession(raceSession))
                    {
                        _idleSessions.Push(raceSession);
                    }
                }

                return session;
            }

            await using var _ = finalToken.Register(() => waiterTcs.TrySetCanceled(), useSynchronizationContext: false);
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
        for (var numSessions = _numSessions; numSessions < _maxSessionSize; numSessions = _numSessions)
        {
            if (Interlocked.CompareExchange(ref _numSessions, numSessions + 1, numSessions) != numSessions)
                continue;

            try
            {
                var session = _sessionFactory.NewSession(this);
                await session.Open(cancellationToken);

                for (var i = 0; i < _maxSessionSize; i++)
                {
                    if (Interlocked.CompareExchange(ref _sessions[i], session, null) == null)
                        return session;
                }

                throw new YdbException(
                    $"Could not find free slot in {_sessions} when opening. Please report a bug.");
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
        if (_waiters.TryDequeue(out var waiter) && waiter.TrySetResult(null))
        {
        } // wake up waiter!
    }

    public void Return(T session)
    {
        if (session.IsBroken)
        {
            CloseSession(session);

            return;
        }

        // Statement order is important
        session.IdleStartTime = DateTime.Now;
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
        for (; i < _maxSessionSize; i++)
            if (Interlocked.CompareExchange(ref _sessions[i], null, session) == session)
                break;

        if (i == _maxSessionSize)
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
        var now = DateTime.Now;

        for (var i = 0; i < pool._maxSessionSize; i++)
        {
            var session = Volatile.Read(ref pool._sessions[i]);

            if (
                session != null &&
                pool._numSessions > pool._minSessionSize &&
                session.IdleStartTime + pool._sessionIdleTimeout <= now &&
                session.CompareAndSet(PoolingSessionState.In, PoolingSessionState.Clean)
            )
            {
                pool.CloseSession(session);
            }
        }
    }
}

internal interface IPoolingSessionFactory<T> where T : PoolingSessionBase<T>
{
    T NewSession(PoolingSessionSource<T> source);
}

internal enum PoolingSessionState
{
    In,
    Out,
    Clean
}

internal abstract class PoolingSessionBase<T> : ISession where T : PoolingSessionBase<T>
{
    private readonly PoolingSessionSource<T> _source;

    private int _state = (int)PoolingSessionState.In;

    protected PoolingSessionBase(PoolingSessionSource<T> source)
    {
        _source = source;
    }

    internal bool CompareAndSet(PoolingSessionState expected, PoolingSessionState actual) =>
        Interlocked.CompareExchange(ref _state, (int)expected, (int)actual) == (int)expected;

    internal void Set(PoolingSessionState state) => Interlocked.Exchange(ref _state, (int)state);

    internal PoolingSessionState State => (PoolingSessionState)Volatile.Read(ref _state);

    internal DateTime IdleStartTime { get; set; }

    public abstract IDriver Driver { get; }

    public abstract bool IsBroken { get; }

    internal abstract Task Open(CancellationToken cancellationToken);

    internal abstract Task DeleteSession();

    public abstract ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(string query,
        Dictionary<string, YdbValue> parameters, GrpcRequestSettings settings,
        TransactionControl? txControl);

    public abstract Task CommitTransaction(string txId, CancellationToken cancellationToken = default);

    public abstract Task RollbackTransaction(string txId, CancellationToken cancellationToken = default);

    public abstract void OnNotSuccessStatusCode(StatusCode code);

    public void Close() => _source.Return((T)this);
}
