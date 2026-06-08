using Xunit;
using Ydb.Coordination;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Recipes;
using static Ydb.Sdk.Coordination.Tests.MockCoordinationStream;

namespace Ydb.Sdk.Coordination.Tests.Recipes;

public class DistributedLockUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task AcquireLockAsync_Acquired_ReturnsHandle()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        var client = CreateClient(scenario);

        await using var locker = await client
            .AcquireLockAsync("/local/coord", "job", "owner"u8.ToArray())
            .WaitAsync(TestTimeout);

        Assert.Equal("job", locker.Name);
        Assert.Equal("owner"u8.ToArray(), locker.Data);

        var acquire = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;
        Assert.Equal("job", acquire.Name);
        Assert.True(acquire.Ephemeral);
        Assert.Equal(ulong.MaxValue, acquire.Count);
    }

    [Fact]
    public async Task AcquireLockAsync_NotAcquired_ThrowsTimeout()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: false)
        };

        var client = CreateClient(scenario);

        var ex = await Assert.ThrowsAsync<YdbException>(() => client
            .AcquireLockAsync("/local/coord", "job", timeout: TimeSpan.FromMilliseconds(50))
            .WaitAsync(TestTimeout));
        Assert.Equal(StatusCode.Timeout, ex.Code);
    }

    [Fact]
    public async Task TryAcquireLockAsync_NotAcquired_ReturnsNull()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: false)
        };

        var client = CreateClient(scenario);

        var locker = await client
            .TryAcquireLockAsync("/local/coord", "job")
            .WaitAsync(TestTimeout);

        Assert.Null(locker);
    }

    [Fact]
    public async Task DisposeAsync_ReleasesSemaphoreAndStopsSession()
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
        var locker = await client.AcquireLockAsync("/local/coord", "job").WaitAsync(TestTimeout);

        await locker.DisposeAsync().AsTask().WaitAsync(TestTimeout);

        Assert.Contains(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.ReleaseSemaphore);
        Assert.Contains(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.SessionStop);
    }

    [Fact]
    public async Task LockLostToken_FiresOnSessionFailure()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        var client = CreateClient(scenario);

        await using var locker = await client.AcquireLockAsync("/local/coord", "job").WaitAsync(TestTimeout);

        Assert.False(locker.LockLostToken.IsCancellationRequested);
        scenario.Enqueue(Failure(StatusIds.Types.StatusCode.SessionExpired));

        await WaitUntil(() => locker.LockLostToken.IsCancellationRequested);
    }

    internal static CoordinationClient CreateClient(MockCoordinationStream scenario)
        => new(scenario.SetupDriver().Object);

    internal static async Task WaitUntil(Func<bool> predicate)
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        while (!predicate())
            await Task.Delay(10, cts.Token);
    }
}
