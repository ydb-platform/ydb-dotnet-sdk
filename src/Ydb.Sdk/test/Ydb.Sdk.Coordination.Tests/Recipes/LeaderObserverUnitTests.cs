using Xunit;
using Ydb.Coordination;
using static Ydb.Sdk.Coordination.Tests.MockCoordinationStream;
using static Ydb.Sdk.Coordination.Tests.Recipes.DistributedLockUnitTests;

namespace Ydb.Sdk.Coordination.Tests.Recipes;

public class LeaderObserverUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task ObserveLeaderAsync_ReturnsCurrentLeader_FromInitialDescribe()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.DescribeSemaphore is null
                ? null
                : DescribeSemaphoreResult(
                    r.DescribeSemaphore.ReqId, "election",
                    watchAdded: true,
                    owners: new[] { Owner(11, "leader-a"u8.ToArray(), orderId: 7) })
        };

        var client = CreateClient(scenario);
        await using var observer = await client
            .ObserveLeaderAsync("/local/coord", "election")
            .WaitAsync(TestTimeout);

        Assert.NotNull(observer.CurrentLeader);
        Assert.Equal(11ul, observer.CurrentLeader!.SessionId);
        Assert.Equal(7ul, observer.CurrentLeader.OrderId);
        Assert.Equal("leader-a"u8.ToArray(), observer.CurrentLeader.Data);

        var describe = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.DescribeSemaphore).DescribeSemaphore;
        Assert.True(describe.IncludeOwners);
        Assert.False(describe.IncludeWaiters);
        Assert.True(describe.WatchOwners);
    }

    [Fact]
    public async Task Observe_StreamsLeaderChanges()
    {
        var scenario = new MockCoordinationStream();
        var describeCount = 0;
        scenario.HandleRequest = r =>
        {
            if (r.DescribeSemaphore is null) return null;
            describeCount++;
            var owner = describeCount == 1
                ? Owner(11, "leader-a"u8.ToArray(), orderId: 7)
                : Owner(22, "leader-b"u8.ToArray(), orderId: 8);
            return DescribeSemaphoreResult(r.DescribeSemaphore.ReqId, "election",
                watchAdded: true, owners: new[] { owner });
        };

        var client = CreateClient(scenario);
        await using var observer = await client
            .ObserveLeaderAsync("/local/coord", "election")
            .WaitAsync(TestTimeout);

        var watchReqId = scenario.Written()
            .Last(r => r.RequestCase == SessionRequest.RequestOneofCase.DescribeSemaphore)
            .DescribeSemaphore.ReqId;

        await using var iterator = observer.ObserveAsync().GetAsyncEnumerator();
        var next = iterator.MoveNextAsync().AsTask();

        scenario.Enqueue(DescribeSemaphoreChanged(watchReqId, dataChanged: false, ownersChanged: true));

        Assert.True(await next.WaitAsync(TestTimeout));
        Assert.NotNull(iterator.Current);
        Assert.Equal(22ul, iterator.Current!.SessionId);
        Assert.Equal("leader-b"u8.ToArray(), iterator.Current.Data);
    }
}
