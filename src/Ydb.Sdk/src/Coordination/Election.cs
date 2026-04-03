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
        var owner = description.Owners.FirstOrDefault();
        return owner != null ? new LeaderInfo(owner.Data.ToByteArray()) : null;
    }
    
    private static bool IsSameLeader(
        (ulong sessionId, ulong orderId)? left,
        (ulong sessionId, ulong orderId)? right)
    {
        if (!left.HasValue || !right.HasValue)
            return left.Equals(right);

        return left.Value.sessionId == right.Value.sessionId &&
               left.Value.orderId == right.Value.orderId;
    }
}
