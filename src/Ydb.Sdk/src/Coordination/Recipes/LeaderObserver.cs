using System.Runtime.CompilerServices;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Subscribes to leader-change notifications for a named election without participating in it.
/// </summary>
/// <remarks>
/// Owns its own <see cref="CoordinationSession"/>. The <see cref="CurrentLeader"/> snapshot
/// is taken at subscription time and refreshed on every change; <see cref="ObserveAsync"/>
/// yields each new leader (or <c>null</c> when there is no current leader).
/// </remarks>
public sealed class LeaderObserver : IAsyncDisposable
{
    private readonly CoordinationSession _session;
    private readonly WatchResult<SemaphoreDescription> _watch;
    private LeaderInfo? _currentLeader;

    private LeaderObserver(
        CoordinationSession session,
        string electionName,
        WatchResult<SemaphoreDescription> watch)
    {
        _session = session;
        ElectionName = electionName;
        _watch = watch;
        _currentLeader = ExtractLeader(watch.Initial);
    }

    /// <summary>Name of the election.</summary>
    public string ElectionName { get; }

    /// <summary>Most recently observed leader, or <c>null</c> if no candidate currently holds leadership.</summary>
    public LeaderInfo? CurrentLeader => _currentLeader;

    /// <summary>
    /// Streams subsequent leader changes. Yields a value whenever the leader's identity changes
    /// (a different owner or no owner at all). Re-yields the same leader's payload if it changes.
    /// </summary>
    public async IAsyncEnumerable<LeaderInfo?> ObserveAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        LeaderInfo? previous = _currentLeader;
        await foreach (var description in _watch.Updates.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var leader = ExtractLeader(description);
            _currentLeader = leader;

            if (Equals(previous, leader)) continue;
            previous = leader;
            yield return leader;
        }
    }

    public async ValueTask DisposeAsync() => await _session.DisposeAsync().ConfigureAwait(false);

    internal static async Task<LeaderObserver> OpenAsync(
        CoordinationClient client,
        string nodePath,
        string electionName,
        CancellationToken cancellationToken)
    {
        var session = await client.OpenSessionAsync(nodePath, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        try
        {
            // Observer doesn't participate, but it needs the semaphore to exist for the watch to
            // attach. Create idempotently — campaigns do the same.
            await Leadership.EnsureElectionSemaphoreAsync(session, electionName, cancellationToken)
                .ConfigureAwait(false);

            var watch = await session.WatchSemaphoreAsync(
                    electionName,
                    DescribeSemaphoreMode.WithOwners,
                    WatchSemaphoreMode.WatchOwners,
                    cancellationToken)
                .ConfigureAwait(false);

            return new LeaderObserver(session, electionName, watch);
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static LeaderInfo? ExtractLeader(SemaphoreDescription description)
    {
        var owner = description.OwnersList.FirstOrDefault();
        return owner is null
            ? null
            : new LeaderInfo(owner.Id, owner.OrderId, owner.Data);
    }
}
