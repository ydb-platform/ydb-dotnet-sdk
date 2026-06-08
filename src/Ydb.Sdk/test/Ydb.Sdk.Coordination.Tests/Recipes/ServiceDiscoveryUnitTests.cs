using Xunit;
using Ydb.Coordination;
using static Ydb.Sdk.Coordination.Tests.MockCoordinationStream;
using static Ydb.Sdk.Coordination.Tests.Recipes.DistributedLockUnitTests;

namespace Ydb.Sdk.Coordination.Tests.Recipes;

public class ServiceRegistrationAndDiscoveryUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task RegisterServiceAsync_AcquiresEphemeralCountOne_WithEndpointAsData()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        var client = CreateClient(scenario);
        await using var registration = await client
            .RegisterServiceAsync("/local/coord", "svc", "host:1"u8.ToArray())
            .WaitAsync(TestTimeout);

        var acquire = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;
        Assert.Equal("svc", acquire.Name);
        Assert.Equal(1ul, acquire.Count);
        Assert.True(acquire.Ephemeral);
        Assert.Equal("host:1"u8.ToArray(), acquire.Data.ToByteArray());
        Assert.Equal("host:1"u8.ToArray(), registration.Endpoint);
    }

    [Fact]
    public async Task DiscoverServiceAsync_ReturnsCurrentEndpoints()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.DescribeSemaphore is null
                ? null
                : DescribeSemaphoreResult(
                    r.DescribeSemaphore.ReqId, "svc",
                    watchAdded: true,
                    owners: new[]
                    {
                        Owner(11, "host:1"u8.ToArray()),
                        Owner(22, "host:2"u8.ToArray())
                    })
        };

        var client = CreateClient(scenario);
        await using var discovery = await client
            .DiscoverServiceAsync("/local/coord", "svc")
            .WaitAsync(TestTimeout);

        Assert.Equal(2, discovery.CurrentEndpoints.Count);
        Assert.Equal("host:1"u8.ToArray(), discovery.CurrentEndpoints[0]);
        Assert.Equal("host:2"u8.ToArray(), discovery.CurrentEndpoints[1]);
    }

    [Fact]
    public async Task DiscoverServiceAsync_Observe_StreamsMembershipChanges()
    {
        var scenario = new MockCoordinationStream();
        var describeCount = 0;
        scenario.HandleRequest = r =>
        {
            if (r.DescribeSemaphore is null) return null;
            describeCount++;
            var owners = describeCount == 1
                ? new[] { Owner(11, "host:1"u8.ToArray()) }
                : new[] { Owner(11, "host:1"u8.ToArray()), Owner(22, "host:2"u8.ToArray()) };
            return DescribeSemaphoreResult(r.DescribeSemaphore.ReqId, "svc",
                watchAdded: true, owners: owners);
        };

        var client = CreateClient(scenario);
        await using var discovery = await client
            .DiscoverServiceAsync("/local/coord", "svc")
            .WaitAsync(TestTimeout);

        Assert.Single(discovery.CurrentEndpoints);

        var watchReqId = scenario.Written()
            .Last(r => r.RequestCase == SessionRequest.RequestOneofCase.DescribeSemaphore)
            .DescribeSemaphore.ReqId;

        await using var iterator = discovery.ObserveAsync().GetAsyncEnumerator();
        var next = iterator.MoveNextAsync().AsTask();

        scenario.Enqueue(DescribeSemaphoreChanged(watchReqId, dataChanged: false, ownersChanged: true));

        Assert.True(await next.WaitAsync(TestTimeout));
        Assert.Equal(2, iterator.Current.Count);
        Assert.Equal("host:2"u8.ToArray(), iterator.Current[1]);
    }
}
