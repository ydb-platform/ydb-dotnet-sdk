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

    public async Task<Leadership> Campaign(byte[] data, CancellationToken ct = default)
    {
        var lease = await _semaphore.Acquire(1, true, data, TimeSpan.FromSeconds(30)); // подправить время
        return new Leadership(_semaphore, lease);
    }

    public async Task<LeaderInfo?> Leader(CancellationToken ct = default)
    {
        var description = await _semaphore.Describe(DescribeSemaphoreMode.WithOwners);
        var owner = description.GetOwnersList().FirstOrDefault();
        return owner != null ? new LeaderInfo(owner.Data) : null;
    }

    /*
    public async IAsyncEnumerable<LeaderState> ObserveAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"observing leadership changes on {Name}");

        LeaderIdentity? previousLeader = null;
        CancellationTokenSource? currentCts = null;


        try
        {
            await foreach(var description in _semaphore.WatchSemaphore(DescribeSemaphoreMode.WithOwners, WatchSemaphoreMode.WatchOwners))
            {

                var owner = description.GetOwnersList().FirstOrDefault();

                LeaderIdentity? currentLeader = owner != null
                    ? new LeaderIdentity(owner.Id, owner.OrderId)
                    : null;

                if (IsSameLeader(previousLeader, currentLeader))
                {
                    continue;
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

                    yield return new LeaderState(
                        Array.Empty<byte>(),
                        false,
                        currentCts.Token
                    );
                    continue;
                }

                var sessionId1 = _semaphore.SessionId;
                var isMe1 = owner.Id == sessionId1;

                yield return new LeaderState
                (
                    owner.Data,
                    isMe1,
                    currentCts.Token
                );
            }
        }

        finally
        {
            if (currentCts != null)
            {
                currentCts.Cancel();
                currentCts.Dispose();
            }
            Console.WriteLine($"stopped observing {Name}");
        }
    }


    private static bool IsSameLeader(
        LeaderIdentity? left,
        LeaderIdentity? right)
        => Equals(left, right);
    */
}

/*
public class Election
{
    private readonly Semaphore _semaphore;

    public Election(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }

    public string Name => _semaphore.Name;

    public async Task<Leadership> Campaign(byte[] data, CancellationToken ct = default)
    {
        var lease = await _semaphore.Acquire(1, true, data, TimeSpan.FromSeconds(30));
        return new Leadership(_semaphore, lease);
    }

    public async Task<LeaderInfo?> Leader(CancellationToken ct = default)
    {
        var description = await _semaphore.Describe(DescribeSemaphoreMode.WithOwners);
        var owner = description.Owners.FirstOrDefault();

        return owner != null
            ? new LeaderInfo(owner.Data.ToByteArray())
            : null;
    }

    public async IAsyncEnumerable<LeaderState> ObserveAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        Console.WriteLine($"observing leadership changes on {Name}");

        LeaderIdentity? previousLeader = null;
        CancellationTokenSource? currentCts = null;

        try
        {
            var watch = await _semaphore.SessionRuntime.WatchSemaphoreSafe(
                _semaphore.Name,
                DescribeSemaphoreMode.WithOwners,
                WatchSemaphoreMode.WatchOwners,
                ct);

            // 🔹 1. initial snapshot
            yield return HandleState(
                watch.Initial,
                ref previousLeader,
                ref currentCts);

            // 🔹 2. updates
            await foreach (var description in watch.Updates.WithCancellation(ct))
            {
                var state = HandleState(
                    description,
                    ref previousLeader,
                    ref currentCts);

                if (state != null)
                {
                    yield return state;
                }
            }
        }
        finally
        {
            currentCts?.Cancel();
            currentCts?.Dispose();

            Console.WriteLine($"stopped observing {Name}");
        }
    }

    private LeaderState? HandleState(
        SemaphoreDescriptionClient description,
        ref LeaderIdentity? previousLeader,
        ref CancellationTokenSource? currentCts)
    {
        var owner = description.Owners.FirstOrDefault();

        LeaderIdentity? currentLeader = owner != null
            ? new LeaderIdentity(owner.Id, owner.OrderId)
            : null;

        if (IsSameLeader(previousLeader, currentLeader))
        {
            return null;
        }

        previousLeader = currentLeader;

        currentCts?.Cancel();
        currentCts?.Dispose();
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

        var isMe = owner.Id == _semaphore.SessionId;

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
*/
