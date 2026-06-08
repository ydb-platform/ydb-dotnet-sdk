using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination;

/// <summary>
/// Handle returned by <see cref="CoordinationSession.AcquireSemaphoreAsync"/>.
/// Releasing the lease (explicitly or via <see cref="IAsyncDisposable.DisposeAsync"/>) frees the semaphore slot.
/// </summary>
public sealed class Lease : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly CancellationTokenSource _lostCts;
    private readonly CancellationTokenRegistration _sessionLostRegistration;

    private int _released;

    internal Lease(CoordinationSession session, string name)
    {
        _session = session;
        Name = name;
        _lostCts = new CancellationTokenSource();
        _sessionLostRegistration = session.SessionLostToken.Register(static state =>
        {
            var l = (Lease)state!;
            try { l._lostCts.Cancel(); }
            catch (ObjectDisposedException) { }
        }, this);
    }

    /// <summary>Name of the acquired semaphore.</summary>
    public string Name { get; }

    /// <summary>The session that owns this lease.</summary>
    public CoordinationSession Session => _session;

    /// <summary>
    /// Cancelled when the lease is released (explicitly or via dispose) or when the owning session
    /// is permanently lost.
    /// </summary>
    public CancellationToken LeaseLostToken => _lostCts.Token;

    /// <summary>
    /// Explicitly releases the lease. Idempotent.
    /// </summary>
    public async Task ReleaseAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;

        try
        {
            await _session.ReleaseSemaphoreAsync(Name, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            try { _lostCts.Cancel(); }
            catch (ObjectDisposedException) { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // Best-effort release. If the session is already dead the worker will surface a
            // YdbException; that's fine — the server has either already cleaned up (ephemeral)
            // or will clean up after the session grace period (non-ephemeral). Capped to a
            // few seconds so a stuck channel doesn't pin the caller forever.
            await ReleaseAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(2))
                .ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
        }
        catch (YdbException)
        {
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            try { _lostCts.Cancel(); }
            catch (ObjectDisposedException) { }

            _sessionLostRegistration.Dispose();
            _lostCts.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
