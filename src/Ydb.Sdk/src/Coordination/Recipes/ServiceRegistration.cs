using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Handle representing this process's registration in a named service. Disposing the handle
/// removes the registration so consumers immediately stop seeing this endpoint.
/// </summary>
/// <remarks>
/// Each instance acquires <c>count=1</c> ephemerally from a shared semaphore named after the
/// service. The endpoint payload is stored in the owner record so consumers can discover all
/// active members through <see cref="ServiceDiscovery"/>.
/// </remarks>
public sealed class ServiceRegistration : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly Lease _lease;
    private int _disposed;

    private ServiceRegistration(CoordinationSession session, Lease lease, byte[] endpoint)
    {
        _session = session;
        _lease = lease;
        Endpoint = endpoint;
        ServiceName = lease.Name;
    }

    /// <summary>Service name (semaphore name).</summary>
    public string ServiceName { get; }

    /// <summary>Endpoint payload published to consumers.</summary>
    public byte[] Endpoint { get; }

    /// <summary>Server-assigned session id for this registration.</summary>
    public ulong SessionId => _session.SessionId;

    /// <summary>Cancelled when this registration is removed (dispose) or the session is lost.</summary>
    public CancellationToken RegistrationLostToken => _lease.LeaseLostToken;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { await _lease.DisposeAsync().ConfigureAwait(false); }
        finally { await _session.DisposeAsync().ConfigureAwait(false); }
    }

    internal static async Task<ServiceRegistration> RegisterAsync(
        CoordinationClient client,
        string nodePath,
        string serviceName,
        byte[] endpoint,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var lease = await session.AcquireSemaphoreAsync(
                    serviceName,
                    count: 1,
                    ephemeral: true,
                    data: endpoint,
                    timeout: null,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            if (lease is null)
                throw new YdbException(StatusCode.InternalError,
                    "Server returned non-acquired result for an infinite-wait Acquire");

            return new ServiceRegistration(session, lease, endpoint);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
