using System.Runtime.CompilerServices;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Discovery handle returned by <see cref="CoordinationClient.DiscoverServiceAsync"/>. Exposes
/// the current list of registered endpoints and streams the membership changes.
/// </summary>
public sealed class ServiceDiscovery : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly WatchResult<SemaphoreDescription> _watch;
    private IReadOnlyList<byte[]> _currentEndpoints;

    private ServiceDiscovery(
        CoordinationSession session,
        string serviceName,
        WatchResult<SemaphoreDescription> watch)
    {
        _session = session;
        ServiceName = serviceName;
        _watch = watch;
        _currentEndpoints = ExtractEndpoints(watch.Initial);
    }

    public string ServiceName { get; }

    /// <summary>Snapshot of the most recently observed endpoints.</summary>
    public IReadOnlyList<byte[]> CurrentEndpoints => _currentEndpoints;

    /// <summary>
    /// Streams subsequent membership changes. Each yield is a complete list of currently-known endpoints.
    /// </summary>
    public async IAsyncEnumerable<IReadOnlyList<byte[]>> ObserveAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var description in _watch.Updates.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var endpoints = ExtractEndpoints(description);
            _currentEndpoints = endpoints;
            yield return endpoints;
        }
    }

    public async ValueTask DisposeAsync() => await _session.DisposeAsync().ConfigureAwait(false);

    internal static async Task<ServiceDiscovery> OpenAsync(
        CoordinationClient client,
        string nodePath,
        string serviceName,
        CancellationToken cancellationToken)
    {
        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            var watch = await session.WatchSemaphoreAsync(
                    serviceName,
                    DescribeSemaphoreMode.WithOwners,
                    WatchSemaphoreMode.WatchOwners,
                    cancellationToken)
                .ConfigureAwait(false);

            return new ServiceDiscovery(session, serviceName, watch);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static IReadOnlyList<byte[]> ExtractEndpoints(SemaphoreDescription description)
        => description.OwnersList.Select(o => o.Data).ToArray();
}
