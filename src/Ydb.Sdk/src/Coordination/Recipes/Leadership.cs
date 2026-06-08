using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Handle returned by <see cref="CoordinationClient.CampaignAsync"/> once this candidate has won
/// the election. Lives on its own <see cref="CoordinationSession"/>; while the handle is alive
/// this process is the leader.
/// </summary>
/// <remarks>
/// <para>Leadership is built on a non-ephemeral semaphore with <c>limit=1</c>: the first candidate
/// to <c>AcquireSemaphore</c> wins. Other candidates queue server-side as waiters and are promoted
/// in order when the current leader resigns or its session dies.</para>
/// <para>
/// <see cref="LeadershipLostToken"/> is cancelled once leadership is lost — either by explicit
/// <see cref="ResignAsync"/>, <see cref="DisposeAsync"/>, or by the underlying session expiring.
/// Long-running leader work should be linked to this token so the leader gracefully aborts when
/// it is no longer the leader.
/// </para>
/// </remarks>
public sealed class Leadership : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly Lease _lease;
    private int _disposed;

    private Leadership(CoordinationSession session, Lease lease, byte[] data)
    {
        _session = session;
        _lease = lease;
        Name = lease.Name;
        Data = data;
    }

    /// <summary>Name of the election (semaphore).</summary>
    public string Name { get; }

    /// <summary>The payload most recently <see cref="ProclaimAsync">proclaimed</see> by this leader.</summary>
    public byte[] Data { get; private set; }

    /// <summary>Server-assigned session id of the leader.</summary>
    public ulong SessionId => _session.SessionId;

    /// <summary>Cancelled when leadership is lost (resign, dispose, or session expiry).</summary>
    public CancellationToken LeadershipLostToken => _lease.LeaseLostToken;

    /// <summary>
    /// Re-publishes the leader payload so observers see fresh data without changing the leader.
    /// </summary>
    public async Task ProclaimAsync(byte[] data, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);
        await _session.UpdateSemaphoreAsync(Name, data, cancellationToken).ConfigureAwait(false);
        Data = data;
    }

    /// <summary>
    /// Explicitly steps down. Idempotent.
    /// </summary>
    public async Task ResignAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { await _lease.ReleaseAsync(cancellationToken).ConfigureAwait(false); }
        finally { await _session.DisposeAsync().ConfigureAwait(false); }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { await _lease.DisposeAsync().ConfigureAwait(false); }
        finally { await _session.DisposeAsync().ConfigureAwait(false); }
    }

    internal static async Task<Leadership> CampaignAsync(
        CoordinationClient client,
        string nodePath,
        string electionName,
        byte[] data,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(data);

        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var lease = await session.AcquireSemaphoreAsync(
                    electionName,
                    count: 1,
                    ephemeral: false,
                    data: data,
                    timeout: null,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (lease is null)
                throw new YdbException(StatusCode.InternalError,
                    "Server returned non-acquired result for an infinite-wait Acquire");

            return new Leadership(session, lease, data);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
