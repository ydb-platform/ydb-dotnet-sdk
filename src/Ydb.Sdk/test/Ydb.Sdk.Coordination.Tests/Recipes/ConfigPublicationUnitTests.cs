using Xunit;
using Ydb.Coordination;
using static Ydb.Sdk.Coordination.Tests.MockCoordinationStream;
using static Ydb.Sdk.Coordination.Tests.Recipes.DistributedLockUnitTests;

namespace Ydb.Sdk.Coordination.Tests.Recipes;

public class ConfigPublicationUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task PublishConfigAsync_AcquiresCountOneNonEphemeral()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        var client = CreateClient(scenario);
        await using var publisher = await client
            .PublishConfigAsync("/local/coord", "cfg", "v1"u8.ToArray())
            .WaitAsync(TestTimeout);

        var acquire = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;
        Assert.Equal("cfg", acquire.Name);
        Assert.False(acquire.Ephemeral);
        Assert.Equal(1ul, acquire.Count);
        Assert.Equal("v1"u8.ToArray(), acquire.Data.ToByteArray());
        Assert.Equal("v1"u8.ToArray(), publisher.CurrentValue);
    }

    [Fact]
    public async Task UpdateAsync_SendsUpdateSemaphore_AndTracksCurrentValue()
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
        await using var publisher = await client
            .PublishConfigAsync("/local/coord", "cfg", "v1"u8.ToArray())
            .WaitAsync(TestTimeout);

        await publisher.UpdateAsync("v2"u8.ToArray()).WaitAsync(TestTimeout);

        Assert.Equal("v2"u8.ToArray(), publisher.CurrentValue);
        var update = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.UpdateSemaphore).UpdateSemaphore;
        Assert.Equal("v2"u8.ToArray(), update.Data.ToByteArray());
    }

    [Fact]
    public async Task SubscribeConfigAsync_StreamsValueChanges()
    {
        var scenario = new MockCoordinationStream();
        var describeCount = 0;
        scenario.HandleRequest = r =>
        {
            if (r.DescribeSemaphore is null) return null;
            describeCount++;
            var data = describeCount == 1 ? "v1"u8.ToArray() : "v2"u8.ToArray();
            return DescribeSemaphoreResult(r.DescribeSemaphore.ReqId, "cfg", data, watchAdded: true);
        };

        var client = CreateClient(scenario);
        await using var subscription = await client
            .SubscribeConfigAsync("/local/coord", "cfg")
            .WaitAsync(TestTimeout);

        Assert.Equal("v1"u8.ToArray(), subscription.CurrentValue);

        var watchReqId = scenario.Written()
            .Last(r => r.RequestCase == SessionRequest.RequestOneofCase.DescribeSemaphore)
            .DescribeSemaphore.ReqId;

        await using var iterator = subscription.ObserveAsync().GetAsyncEnumerator();
        var next = iterator.MoveNextAsync().AsTask();

        scenario.Enqueue(DescribeSemaphoreChanged(watchReqId, dataChanged: true, ownersChanged: false));

        Assert.True(await next.WaitAsync(TestTimeout));
        Assert.Equal("v2"u8.ToArray(), iterator.Current);
        Assert.Equal("v2"u8.ToArray(), subscription.CurrentValue);
    }
}
