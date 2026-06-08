using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// A distributed mutual exclusion lock backed by an exclusive ephemeral acquire on a YDB
/// coordination semaphore. The lock is held until <see cref="DisposeAsync"/> /
/// <see cref="ReleaseAsync"/> is called, the owning session dies, or the server expires
/// the underlying ephemeral semaphore.
/// </summary>
/// <remarks>
/// <para>Each <see cref="DistributedLock"/> owns its own <see cref="CoordinationSession"/>.
/// Disposing the lock closes the session, which automatically releases the server-side
/// ephemeral semaphore even if the network is mid-flight.</para>
/// <para>The <see cref="LockLostToken"/> is cancelled if the session is irrecoverably lost —
/// callers should treat this as "I no longer hold the lock" and abort the protected work.</para>
/// </remarks>
public sealed class DistributedLock : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly Lease _lease;
    private int _disposed;

    private DistributedLock(CoordinationSession session, Lease lease, byte[] data)
    {
        _session = session;
        _lease = lease;
        Data = data;
        Name = lease.Name;
    }

    /// <summary>Name of the lock (semaphore).</summary>
    public string Name { get; }

    /// <summary>Arbitrary payload attached to the lock owner record on the server.</summary>
    public byte[] Data { get; }

    /// <summary>Cancelled when the lock is released, disposed, or the underlying session is lost.</summary>
    public CancellationToken LockLostToken => _lease.LeaseLostToken;

    /// <summary>Server-assigned session id behind this lock. Useful for diagnostics.</summary>
    public ulong SessionId => _session.SessionId;

    public async Task ReleaseAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            await _lease.ReleaseAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await _session.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            await _lease.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            await _session.DisposeAsync().ConfigureAwait(false);
        }
    }

    internal static async Task<DistributedLock> AcquireAsync(
        CoordinationClient client,
        string nodePath,
        string lockName,
        byte[]? data,
        TimeSpan? timeout,
        CancellationToken cancellationToken)
    {
        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var lease = await session
                .AcquireSemaphoreAsync(lockName, ulong.MaxValue, ephemeral: true, data: data,
                    timeout: timeout, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (lease is null)
            {
                await session.DisposeAsync().ConfigureAwait(false);
                throw new YdbException(StatusCode.Timeout,
                    $"AcquireLockAsync({lockName}) timed out after {timeout}");
            }

            return new DistributedLock(session, lease, data ?? []);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal static async Task<DistributedLock?> TryAcquireAsync(
        CoordinationClient client,
        string nodePath,
        string lockName,
        byte[]? data,
        CancellationToken cancellationToken)
    {
        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var lease = await session
                .AcquireSemaphoreAsync(lockName, ulong.MaxValue, ephemeral: true, data: data,
                    timeout: TimeSpan.Zero, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (lease is null)
            {
                await session.DisposeAsync().ConfigureAwait(false);
                return null;
            }

            return new DistributedLock(session, lease, data ?? []);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
