using System.Collections.Concurrent;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Ydb.Sdk.Ado.Session;

internal sealed class PoolingSessionSource : ISessionSource<PoolingSession>
{
    private readonly ConcurrentStack<PoolingSession> _idleSessions = new();
    private readonly ConcurrentQueue<TaskCompletionSource<PoolingSession?>> _waiters = new();

    private readonly IPoolingSessionFactory _sessionFactory;

    private readonly int _minSessionSize;
    private readonly int _maxSessionSize;

    private readonly PoolingSession?[] _sessions;

    private readonly int _createSessionTimeout;
    private readonly TimeSpan _sessionIdleTimeout;
    private readonly Timer _cleanerTimer;

    private volatile int _numSessions;

    public PoolingSessionSource(
        IPoolingSessionFactory sessionFactory,
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

        _sessions = new PoolingSession?[_maxSessionSize];
        _createSessionTimeout = settings.CreateSessionTimeout;
        _sessionIdleTimeout = TimeSpan.FromSeconds(settings.SessionIdleTimeout);
        _cleanerTimer = new Timer(CleanIdleSessions, this, _sessionIdleTimeout, _sessionIdleTimeout);
    }

    public ValueTask<PoolingSession> OpenSession(CancellationToken cancellationToken = default) =>
        TryGetIdleSession(out var session)
            ? new ValueTask<PoolingSession>(session)
            : RentAsync(cancellationToken);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryGetIdleSession([NotNullWhen(true)] out PoolingSession? session)
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
    private bool CheckIdleSession([NotNullWhen(true)] PoolingSession? session)
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

    private async ValueTask<PoolingSession> RentAsync(CancellationToken cancellationToken)
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
            var waiterTcs =
                new TaskCompletionSource<PoolingSession?>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Enqueue(waiterTcs);
            await using var _ = finalToken.Register(() => waiterTcs.TrySetCanceled(), useSynchronizationContext: false);
            session = await waiterTcs.Task.ConfigureAwait(false);

            if (CheckIdleSession(session) || TryGetIdleSession(out session))
                return session;

            session = await OpenNewSession(finalToken).ConfigureAwait(false);
            if (session != null)
                return session;
        }
    }

    private async ValueTask<PoolingSession?> OpenNewSession(CancellationToken cancellationToken)
    {
        // As long as we're under max capacity, attempt to increase the session count and open a new session.
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
                // RPC open failed, decrement the open and busy counter back down.
                Interlocked.Decrement(ref _numSessions);

                // In case there's a waiting attempt on the waiters queue, we write a null to the idle connector channel
                // to wake it up, so it will try opening (and probably throw immediately)
                // Statement order is important since we have synchronous completions on the channel.
                WakeUpWaiter();

                throw;
            }
        }

        return null;
    }

    private void WakeUpWaiter()
    {
        if (_waiters.TryDequeue(out var waiter))
            waiter.TrySetResult(null); // wake up waiter!
    }

    public void Return(PoolingSession session)
    {
        if (session.IsBroken)
        {
            CloseSession(session);

            return;
        }

        // Statement order is important
        session.IdleStartTime = DateTime.Now;
        session.Set(PoolingSessionState.In);

        if (_waiters.TryDequeue(out var waiter))
        {
            waiter.TrySetResult(session);

            return;
        }

        _idleSessions.Push(session);

        WakeUpWaiter();
    }

    private void CloseSession(PoolingSession session)
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
        var pool = (PoolingSessionSource)state!;
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

internal interface IPoolingSessionFactory
{
    PoolingSession NewSession(PoolingSessionSource source);
}

internal enum PoolingSessionState
{
    In,
    Out,
    Clean
}
