using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Exclusive publisher of a named configuration entry.
/// </summary>
/// <remarks>
/// <para>The publisher acquires a non-ephemeral semaphore with <c>count=1</c> — only one
/// publisher is active at a time. The configuration value is stored in the semaphore's
/// data payload and propagated to subscribers via <see cref="ConfigSubscription"/>.</para>
/// <para>Disposing the publisher resigns from the publisher role; another candidate can then take over.</para>
/// </remarks>
public sealed class ConfigPublisher : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly Lease _lease;
    private int _disposed;

    private ConfigPublisher(CoordinationSession session, Lease lease, byte[] initial)
    {
        _session = session;
        _lease = lease;
        ConfigName = lease.Name;
        CurrentValue = initial;
    }

    /// <summary>Configuration name (semaphore name).</summary>
    public string ConfigName { get; }

    /// <summary>The most recently published value.</summary>
    public byte[] CurrentValue { get; private set; }

    /// <summary>Cancelled when the publisher is disposed or its session expires.</summary>
    public CancellationToken PublisherLostToken => _lease.LeaseLostToken;

    /// <summary>Atomically updates the published value.</summary>
    public async Task UpdateAsync(byte[] value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        await _session.UpdateSemaphoreAsync(ConfigName, value, cancellationToken).ConfigureAwait(false);
        CurrentValue = value;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { await _lease.DisposeAsync().ConfigureAwait(false); }
        finally { await _session.DisposeAsync().ConfigureAwait(false); }
    }

    internal static async Task<ConfigPublisher> OpenAsync(
        CoordinationClient client,
        string nodePath,
        string configName,
        byte[] initialValue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(initialValue);

        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var lease = await session.AcquireSemaphoreAsync(
                    configName,
                    count: 1,
                    ephemeral: false,
                    data: initialValue,
                    timeout: null,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (lease is null)
                throw new YdbException(StatusCode.InternalError,
                    "Server returned non-acquired result for an infinite-wait Acquire");

            return new ConfigPublisher(session, lease, initialValue);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
