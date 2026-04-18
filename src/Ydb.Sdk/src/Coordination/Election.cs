using System.Runtime.CompilerServices;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Dto;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class Election
{
    private readonly Semaphore _semaphore;

    public Election(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }

    public string Name
        => _semaphore.Name;

    public async Task<Leadership> Campaign(byte[] data, CancellationToken cancellationToken = default)
    {
        var lease = await _semaphore.Acquire(1, false, data, null, cancellationToken);
        return new Leadership(_semaphore, lease);
    }

    public async Task<LeaderInfo?> Leader(CancellationToken cancellationToken = default)
    {
        var description = await _semaphore.Describe(DescribeSemaphoreMode.WithOwners, cancellationToken);
        var owner = description.GetOwnersList().FirstOrDefault();
        return owner != null ? new LeaderInfo(owner.Data) : null;
    }


    public async IAsyncEnumerable<LeaderState> Observe(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"observing leadership changes on {Name}");

        LeaderIdentity? previousLeader = null;
        CancellationTokenSource? currentCts = null;

        try
        {
            var watch = await _semaphore.WatchSemaphore(DescribeSemaphoreMode.WithOwners,
                WatchSemaphoreMode.WatchOwners, cancellationToken);
            var stateInitial = HandleState(watch.Initial, ref previousLeader, ref currentCts);
            if (stateInitial != null)
            {
                yield return stateInitial;
            }

            await foreach (var description in watch.Updates.WithCancellation(cancellationToken))
            {
                var state = HandleState(description, ref previousLeader, ref currentCts);
                if (state != null)
                {
                    yield return state;
                }
            }
        }
        finally
        {
            if (currentCts != null)
            {
                await currentCts.CancelAsync();
                currentCts.Dispose();
            }

            Console.WriteLine($"stopped observing {Name}");
        }
    }

    private LeaderState? HandleState(
        SemaphoreDescriptionClient description,
        ref LeaderIdentity? previousLeader,
        ref CancellationTokenSource? currentCts)
    {
        var owner = description.GetOwnersList().FirstOrDefault();

        var currentLeader = owner != null
            ? new LeaderIdentity(owner.Id, owner.OrderId)
            : null;

        if (IsSameLeader(previousLeader, currentLeader))
        {
            return null;
        }

        previousLeader = currentLeader;

        if (currentCts != null)
        {
            currentCts.Cancel();
            currentCts.Dispose();
        }

        currentCts = new CancellationTokenSource();

        if (owner == null)
        {
            Console.WriteLine($"no leader on {Name}");

            return new LeaderState(
                Array.Empty<byte>(),
                false,
                currentCts.Token
            );
        }

        var sessionId = _semaphore.SessionId;
        var isMe = owner.Id == sessionId;

        return new LeaderState(
            owner.Data,
            isMe,
            currentCts.Token
        );
    }

    private static bool IsSameLeader(
        LeaderIdentity? left,
        LeaderIdentity? right)
        => Equals(left, right);
}
