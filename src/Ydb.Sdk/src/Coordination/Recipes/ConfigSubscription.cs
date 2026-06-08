using System.Runtime.CompilerServices;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Subscription to a named configuration entry. Exposes the latest value and streams updates
/// whenever the publisher calls <see cref="ConfigPublisher.UpdateAsync"/>.
/// </summary>
public sealed class ConfigSubscription : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly WatchResult<SemaphoreDescription> _watch;
    private byte[] _currentValue;

    private ConfigSubscription(
        CoordinationSession session,
        string configName,
        WatchResult<SemaphoreDescription> watch)
    {
        _session = session;
        ConfigName = configName;
        _watch = watch;
        _currentValue = watch.Initial.Data;
    }

    public string ConfigName { get; }

    /// <summary>Most recently observed configuration value.</summary>
    public byte[] CurrentValue => _currentValue;

    /// <summary>
    /// Streams subsequent values. Yields whenever the publisher updates the configuration.
    /// </summary>
    public async IAsyncEnumerable<byte[]> ObserveAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var description in _watch.Updates.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            _currentValue = description.Data;
            yield return description.Data;
        }
    }

    public async ValueTask DisposeAsync() => await _session.DisposeAsync().ConfigureAwait(false);

    internal static async Task<ConfigSubscription> OpenAsync(
        CoordinationClient client,
        string nodePath,
        string configName,
        CancellationToken cancellationToken)
    {
        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            // The watch needs an existing semaphore — create idempotently with empty data so the
            // subscriber can attach before the publisher has set anything.
            await ConfigPublisher.EnsureConfigSemaphoreAsync(session, configName, initialValue: null,
                cancellationToken).ConfigureAwait(false);

            var watch = await session.WatchSemaphoreAsync(
                    configName,
                    DescribeSemaphoreMode.DataOnly,
                    WatchSemaphoreMode.WatchData,
                    cancellationToken)
                .ConfigureAwait(false);

            return new ConfigSubscription(session, configName, watch);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
