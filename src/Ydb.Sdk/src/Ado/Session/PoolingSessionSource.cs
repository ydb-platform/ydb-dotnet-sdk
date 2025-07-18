// This file contains session pooling algorithms adapted from Npgsql
// Original source: https://github.com/npgsql/npgsql
// Copyright (c) 2002-2025, Npgsql
// Licence https://github.com/npgsql/npgsql?tab=PostgreSQL-1-ov-file

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Session;

internal sealed class PoolingSessionSource : ISessionSource<IPoolingSession>
{
    private readonly ILogger<PoolingSessionSource> _logger;
    private readonly IPoolingSessionFactory _sessionFactory;

    private readonly int _minSessionSize;
    private readonly int _maxSessionSize;

    private readonly ChannelReader<IPoolingSession?> _idleSessionReader;
    private readonly ChannelWriter<IPoolingSession?> _idleSessionWriter;

    private readonly int _createSessionTimeout;

    private readonly Timer _pruningTimer;
    private readonly int _pruningSampleSize;
    private readonly int _pruningMedianIndex;
    private readonly TimeSpan _pruningSamplingInterval;
    private readonly int[] _pruningSamples;
    private volatile bool _pruningTimerEnabled;
    private int _pruningSampleIndex;

    private volatile int _numSessions;
    private volatile int _idleCount;

    public PoolingSessionSource(
        IDriver driver,
        IPoolingSessionFactory sessionFactory,
        YdbConnectionStringBuilder settings
    )
    {
        _logger = driver.LoggerFactory.CreateLogger<PoolingSessionSource>();
        _sessionFactory = sessionFactory;

        _minSessionSize = settings.MinSessionPool;
        _maxSessionSize = settings.MaxSessionPool;
        if (_minSessionSize > _maxSessionSize)
        {
            throw new ArgumentException(
                $"Connection can't have 'Max Session Pool' {_maxSessionSize} under 'Min Session Pool' {_minSessionSize}");
        }

        var channel = Channel.CreateUnbounded<IPoolingSession?>();
        _idleSessionReader = channel.Reader;
        _idleSessionWriter = channel.Writer;

        _createSessionTimeout = settings.CreateSessionTimeout;

        if (settings.SessionPruningInterval > settings.SessionIdleTimeout)
        {
            throw new ArgumentException(
                $"YdbConnection can't have {nameof(settings.SessionIdleTimeout)} {settings.SessionIdleTimeout} (in seconds) under {nameof(settings.SessionPruningInterval)} {settings.SessionPruningInterval} (in seconds)");
        }

        _pruningTimer = new Timer(PruneIdleSessions, this, Timeout.Infinite, Timeout.Infinite);
        _pruningSampleSize = DivideRoundingUp(settings.SessionIdleTimeout, settings.SessionPruningInterval);
        _pruningMedianIndex = DivideRoundingUp(_pruningSampleSize, 2) - 1; // - 1 to go from length to index
        _pruningSamplingInterval = TimeSpan.FromSeconds(settings.SessionPruningInterval);
        _pruningSamples = new int[_pruningSampleSize];
        _pruningTimerEnabled = false;
    }

    public ValueTask<IPoolingSession> OpenSession(CancellationToken cancellationToken) =>
        TryGetIdleSession(out var session) ? new ValueTask<IPoolingSession>(session) : RentAsync(cancellationToken);

    public void Return(IPoolingSession session)
    {
        if (session.IsBroken)
        {
            CloseSession(session);
            return;
        }

        Interlocked.Increment(ref _idleCount);
        _idleSessionWriter.TryWrite(session);
    }

    private async ValueTask<IPoolingSession> RentAsync(CancellationToken cancellationToken)
    {
        using var ctsGetSession = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (_createSessionTimeout > 0)
        {
            ctsGetSession.CancelAfter(TimeSpan.FromSeconds(_createSessionTimeout));
        }

        var finalToken = ctsGetSession.Token;

        try
        {
            var session = await OpenNewSession(finalToken).ConfigureAwait(false);
            if (session != null)
                return session;

            while (true)
            {
                session = await _idleSessionReader.ReadAsync(finalToken).ConfigureAwait(false);

                if (CheckIdleSession(session))
                {
                    return session;
                }

                // If we're here, our waiting attempt on the idle session channel was released with a null
                // (or bad session), or we're in sync mode. Check again if a new idle session has appeared since we last checked.
                if (TryGetIdleSession(out session))
                {
                    return session;
                }

                // We might have closed a session in the meantime and no longer be at max capacity
                // so try to open a new session and if that fails, loop again.
                session = await OpenNewSession(finalToken).ConfigureAwait(false);
                if (session != null)
                {
                    return session;
                }
            }
        }
        catch (OperationCanceledException e)
        {
            throw new YdbException(StatusCode.Cancelled,
                $"The connection pool has been exhausted, either raise 'MaxSessionPool' " +
                $"(currently {_maxSessionSize}) or 'CreateSessionTimeout' " +
                $"(currently {_createSessionTimeout} seconds) in your connection string.", e
            );
        }
    }

    private async ValueTask<IPoolingSession?> OpenNewSession(CancellationToken cancellationToken)
    {
        for (var numSessions = _numSessions; numSessions < _maxSessionSize; numSessions = _numSessions)
        {
            if (Interlocked.CompareExchange(ref _numSessions, numSessions + 1, numSessions) != numSessions)
            {
                continue;
            }

            try
            {
                var session = _sessionFactory.NewSession(this);
                await session.Open(cancellationToken);

                // Only start pruning if we've incremented open count past _min.
                // Note that we don't do it only once, on equality, because the thread which incremented open count past _min might get exception
                // on NpgsqlSession.Open due to timeout, CancellationToken or other reasons.
                if (numSessions >= _minSessionSize)
                {
                    UpdatePruningTimer();
                }

                return session;
            }
            catch
            {
                // Physical open failed, decrement the open and busy counter back down.
                Interlocked.Decrement(ref _numSessions);

                // In case there's a waiting attempt on the channel, we write a null to the idle session channel
                // to wake it up, so it will try opening (and probably throw immediately)
                // Statement order is important since we have synchronous completions on the channel.
                _idleSessionWriter.TryWrite(null);

                // Just in case we always call UpdatePruningTimer for failed physical open
                UpdatePruningTimer();

                throw;
            }
        }

        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryGetIdleSession([NotNullWhen(true)] out IPoolingSession? session)
    {
        while (_idleSessionReader.TryRead(out session))
        {
            if (CheckIdleSession(session))
            {
                return true;
            }
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CheckIdleSession([NotNullWhen(true)] IPoolingSession? session)
    {
        if (session == null)
        {
            return false;
        }

        // Only decrement when the session has a value.
        Interlocked.Decrement(ref _idleCount);

        if (session.IsBroken)
        {
            CloseSession(session);

            return false;
        }

        return true;
    }

    private void CloseSession(IPoolingSession session)
    {
        session.DeleteSession();

        var numSessions = Interlocked.Decrement(ref _numSessions);

        // If a session has been closed for any reason, we write a null to the idle session channel to wake up
        // a waiter, who will open a new physical connection
        // Statement order is important since we have synchronous completions on the channel.
        _idleSessionWriter.TryWrite(null);

        // Only turn off the timer one time, when it was this Close that brought Open back to _min.
        if (numSessions == _minSessionSize)
        {
            UpdatePruningTimer();
        }
    }

    private void UpdatePruningTimer()
    {
        lock (_pruningTimer)
        {
            var numSessions = _numSessions;
            if (numSessions > _minSessionSize && !_pruningTimerEnabled)
            {
                _pruningTimerEnabled = true;
                _pruningTimer.Change(_pruningSamplingInterval, Timeout.InfiniteTimeSpan);
            }
            else if (numSessions <= _minSessionSize && _pruningTimerEnabled)
            {
                _pruningTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _pruningSampleIndex = 0;
                _pruningTimerEnabled = false;
            }
        }
    }

    private static void PruneIdleSessions(object? state)
    {
        var pool = (PoolingSessionSource)state!;
        var samples = pool._pruningSamples;
        int toPrune;
        lock (pool._pruningTimer)
        {
            // Check if we might have been contending with DisablePruning.
            if (!pool._pruningTimerEnabled)
                return;

            var sampleIndex = pool._pruningSampleIndex;
            samples[sampleIndex] = pool._idleCount;
            if (sampleIndex != pool._pruningSampleSize - 1)
            {
                pool._pruningSampleIndex = sampleIndex + 1;
                pool._pruningTimer.Change(pool._pruningSamplingInterval, Timeout.InfiniteTimeSpan);
                return;
            }

            // Calculate median value for pruning, reset index and timer, and release the lock.
            Array.Sort(samples);
            toPrune = samples[pool._pruningMedianIndex];
            pool._pruningSampleIndex = 0;
            pool._pruningTimer.Change(pool._pruningSamplingInterval, Timeout.InfiniteTimeSpan);
        }

        if (pool._logger.IsEnabled(LogLevel.Debug))
        {
        }

        while (toPrune > 0 &&
               pool._numSessions > pool._minSessionSize &&
               pool._idleSessionReader.TryRead(out var session) &&
               session != null)
        {
            if (pool.CheckIdleSession(session))
            {
                pool.CloseSession(session);
            }

            toPrune--;
        }
    }

    private static int DivideRoundingUp(int dividend, int divisor) => (dividend + divisor - 1) / divisor;
}

internal interface IPoolingSessionFactory
{
    IPoolingSession NewSession(PoolingSessionSource source);
}

internal interface IPoolingSession : ISession
{
    Task Open(CancellationToken cancellationToken);

    Task DeleteSession();
}
