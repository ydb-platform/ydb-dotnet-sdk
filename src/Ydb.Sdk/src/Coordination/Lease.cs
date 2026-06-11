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
        // Cache the token at construction time so that reading it after DisposeAsync()
        // does not throw ObjectDisposedException.
        LeaseLostToken = _lostCts.Token;
        _sessionLostRegistration = session.SessionLostToken.Register(static state =>
        {
            var l = (Lease)state!;
            // Fire-and-forget CancelAsync so we don't run user callbacks (registered on
            // LeaseLostToken) on the session reader thread that triggered SessionLostToken.
            _ = l.CancelLostAsync();
        }, this);
    }

    /// <summary>Name of the acquired semaphore.</summary>
    public string Name { get; }

    internal CoordinationSession Session => _session;

    private async Task CancelLostAsync()
    {
        try
        {
            await _lostCts.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    /// <summary>
    /// Cancelled when the lease is released (explicitly or via dispose) or when the owning session
    /// is permanently lost. Safe to read after <see cref="DisposeAsync"/>.
    /// </summary>
    public CancellationToken LeaseLostToken { get; }

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
            try
            {
                await _lostCts.CancelAsync();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // Best-effort release capped to a few seconds so a stuck channel doesn't pin the caller.
            // If the session is already dead the server cleans up (immediately for ephemeral,
            // after the grace period otherwise).
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
            try
            {
                await _lostCts.CancelAsync();
            }
            catch (ObjectDisposedException)
            {
            }

            await _sessionLostRegistration.DisposeAsync();
            _lostCts.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
