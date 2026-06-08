using Xunit;
using Ydb.Coordination;
using static Ydb.Sdk.Coordination.Tests.MockCoordinationStream;
using static Ydb.Sdk.Coordination.Tests.Recipes.DistributedLockUnitTests;

namespace Ydb.Sdk.Coordination.Tests.Recipes;

public class LeadershipUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task CampaignAsync_AcquiresSemaphoreWithCountOneAndNonEphemeral()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        var client = CreateClient(scenario);

        await using var leader = await client
            .CampaignAsync("/local/coord", "election", "host:1"u8.ToArray())
            .WaitAsync(TestTimeout);

        Assert.Equal("election", leader.Name);
        Assert.Equal("host:1"u8.ToArray(), leader.Data);

        var acquire = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;
        Assert.Equal("election", acquire.Name);
        Assert.Equal(1ul, acquire.Count);
        Assert.False(acquire.Ephemeral);
        Assert.Equal("host:1"u8.ToArray(), acquire.Data.ToByteArray());
    }

    [Fact]
    public async Task ProclaimAsync_SendsUpdateSemaphore()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r =>
            {
                if (r.AcquireSemaphore is not null)
                    return AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true);
                if (r.UpdateSemaphore is not null)
                    return UpdateSemaphoreResult(r.UpdateSemaphore.ReqId);
                return null;
            }
        };

        var client = CreateClient(scenario);
        await using var leader = await client
            .CampaignAsync("/local/coord", "election", "v1"u8.ToArray())
            .WaitAsync(TestTimeout);

        await leader.ProclaimAsync("v2"u8.ToArray()).WaitAsync(TestTimeout);

        Assert.Equal("v2"u8.ToArray(), leader.Data);

        var update = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.UpdateSemaphore).UpdateSemaphore;
        Assert.Equal("election", update.Name);
        Assert.Equal("v2"u8.ToArray(), update.Data.ToByteArray());
    }

    [Fact]
    public async Task ResignAsync_ReleasesSemaphore()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r =>
            {
                if (r.AcquireSemaphore is not null)
                    return AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true);
                if (r.ReleaseSemaphore is not null)
                    return ReleaseSemaphoreResult(r.ReleaseSemaphore.ReqId);
                return null;
            }
        };

        var client = CreateClient(scenario);
        var leader = await client
            .CampaignAsync("/local/coord", "election", "leader"u8.ToArray())
            .WaitAsync(TestTimeout);

        await leader.ResignAsync().WaitAsync(TestTimeout);

        Assert.Contains(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.ReleaseSemaphore &&
                 r.ReleaseSemaphore.Name == "election");
    }

    [Fact]
    public async Task LeadershipLostToken_FiresOnSessionFailure()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        var client = CreateClient(scenario);
        await using var leader = await client
            .CampaignAsync("/local/coord", "election", "leader"u8.ToArray())
            .WaitAsync(TestTimeout);

        Assert.False(leader.LeadershipLostToken.IsCancellationRequested);
        scenario.Enqueue(Failure(StatusIds.Types.StatusCode.SessionExpired));

        await WaitUntil(() => leader.LeadershipLostToken.IsCancellationRequested);
    }
}
