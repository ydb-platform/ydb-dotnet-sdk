using Xunit;
using Ydb.Coordination;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Settings;
using static Ydb.Sdk.Coordination.Tests.MockCoordinationStream;

namespace Ydb.Sdk.Coordination.Tests;

public class CoordinationSessionUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Open_SendsSessionStartAndReceivesSessionId()
    {
        var scenario = new MockCoordinationStream();
        await using var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);

        var sessionStart = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.SessionStart).SessionStart;
        Assert.Equal(0ul, sessionStart.SessionId);
        Assert.Equal("/local/coord", sessionStart.Path);
        Assert.NotEmpty(sessionStart.ProtectionKey.ToByteArray());
        Assert.Equal(DefaultSessionId, session.SessionId);
    }

    [Fact]
    public async Task CreateSemaphore_RoundTrip_SendsRequestAndCompletesOnResult()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.CreateSemaphore is null
                ? null
                : CreateSemaphoreResult(r.CreateSemaphore.ReqId)
        };

        await using var session = CreateSession(scenario);
        var data = new byte[] { 1, 2, 3 };

        await session.CreateSemaphoreAsync("sem", 10, data).WaitAsync(TestTimeout);

        var create = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.CreateSemaphore).CreateSemaphore;
        Assert.Equal("sem", create.Name);
        Assert.Equal(10ul, create.Limit);
        Assert.Equal(data, create.Data.ToByteArray());
    }

    [Fact]
    public async Task UpdateSemaphore_RoundTrip()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.UpdateSemaphore is null
                ? null
                : UpdateSemaphoreResult(r.UpdateSemaphore.ReqId)
        };

        await using var session = CreateSession(scenario);

        await session.UpdateSemaphoreAsync("sem", "v2"u8.ToArray()).WaitAsync(TestTimeout);

        var update = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.UpdateSemaphore).UpdateSemaphore;
        Assert.Equal("sem", update.Name);
        Assert.Equal("v2"u8.ToArray(), update.Data.ToByteArray());
    }

    [Fact]
    public async Task DeleteSemaphore_PassesForceFlag()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.DeleteSemaphore is null
                ? null
                : DeleteSemaphoreResult(r.DeleteSemaphore.ReqId)
        };

        await using var session = CreateSession(scenario);

        await session.DeleteSemaphoreAsync("sem", force: true).WaitAsync(TestTimeout);

        var delete = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.DeleteSemaphore).DeleteSemaphore;
        Assert.Equal("sem", delete.Name);
        Assert.True(delete.Force);
    }

    [Fact]
    public async Task DescribeSemaphore_ReturnsParsedDescription()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.DescribeSemaphore is null
                ? null
                : DescribeSemaphoreResult(
                    r.DescribeSemaphore.ReqId, "sem", "data"u8.ToArray(),
                    owners: new[] { Owner(11, "host:1"u8.ToArray()) },
                    waiters: new[] { Owner(22, "host:2"u8.ToArray()) })
        };

        await using var session = CreateSession(scenario);

        var description = await session
            .DescribeSemaphoreAsync("sem", DescribeSemaphoreMode.WithOwnersAndWaiters)
            .WaitAsync(TestTimeout);

        Assert.Equal("sem", description.Name);
        Assert.Equal("data"u8.ToArray(), description.Data);
        Assert.Single(description.OwnersList);
        Assert.Single(description.WaitersList);
        Assert.Equal(11ul, description.OwnersList[0].Id);
    }

    [Fact]
    public async Task AcquireSemaphore_AcquiredTrue_ReturnsLease()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        await using var session = CreateSession(scenario);

        var lease = await session
            .AcquireSemaphoreAsync("lock", count: 1, ephemeral: true, data: "x"u8.ToArray(),
                timeout: TimeSpan.FromSeconds(3))
            .WaitAsync(TestTimeout);

        Assert.NotNull(lease);
        Assert.Equal("lock", lease!.Name);

        var acquire = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;
        Assert.Equal("lock", acquire.Name);
        Assert.Equal(1ul, acquire.Count);
        Assert.True(acquire.Ephemeral);
        Assert.Equal(3000ul, acquire.TimeoutMillis);
    }

    [Fact]
    public async Task AcquireSemaphore_AcquiredFalse_ReturnsNull()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: false)
        };

        await using var session = CreateSession(scenario);

        var lease = await session
            .AcquireSemaphoreAsync("busy", timeout: TimeSpan.Zero)
            .WaitAsync(TestTimeout);

        Assert.Null(lease);
    }

    [Fact]
    public async Task AcquireSemaphore_PendingThenResult_Resolves()
    {
        var pendingSent = new TaskCompletionSource();
        var scenario = new MockCoordinationStream();
        scenario.HandleRequest = r =>
        {
            if (r.AcquireSemaphore is null) return null;

            if (!pendingSent.Task.IsCompleted)
            {
                pendingSent.TrySetResult();
                return AcquireSemaphorePending(r.AcquireSemaphore.ReqId);
            }

            return AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true);
        };

        await using var session = CreateSession(scenario);

        var acquireTask = session.AcquireSemaphoreAsync("lock", timeout: null);
        await pendingSent.Task.WaitAsync(TestTimeout);

        // Push the final result for the same reqId
        var pendingRequest = scenario.Written().Single(r =>
            r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;
        scenario.Enqueue(AcquireSemaphoreResult(pendingRequest.ReqId, acquired: true));

        var lease = await acquireTask.WaitAsync(TestTimeout);
        Assert.NotNull(lease);
    }

    [Fact]
    public async Task ReleaseSemaphore_RoundTrip()
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

        await using var session = CreateSession(scenario);

        var lease = await session.AcquireSemaphoreAsync("lock").WaitAsync(TestTimeout);
        Assert.NotNull(lease);
        await lease!.ReleaseAsync().WaitAsync(TestTimeout);

        Assert.Contains(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.ReleaseSemaphore &&
                 r.ReleaseSemaphore.Name == "lock");
    }

    [Fact]
    public async Task Ping_RepliedWithPong_ContainingSameOpaque()
    {
        var scenario = new MockCoordinationStream();
        await using var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);

        scenario.Enqueue(Ping(987));

        await WaitUntil(() => scenario.Written().Any(r => r.RequestCase == SessionRequest.RequestOneofCase.Pong));

        var pong = Assert.Single(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.Pong).Pong;
        Assert.Equal(987ul, pong.Opaque);
    }

    [Fact]
    public async Task WatchSemaphore_InitialAndUpdates()
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

        await using var session = CreateSession(scenario);

        var watch = await session
            .WatchSemaphoreAsync("cfg", DescribeSemaphoreMode.DataOnly, WatchSemaphoreMode.WatchData)
            .WaitAsync(TestTimeout);

        Assert.Equal("v1"u8.ToArray(), watch.Initial.Data);

        var watchReqId = scenario.Written()
            .Last(r => r.RequestCase == SessionRequest.RequestOneofCase.DescribeSemaphore)
            .DescribeSemaphore.ReqId;

        await using var iterator = watch.Updates.GetAsyncEnumerator();
        var next = iterator.MoveNextAsync().AsTask();
        Assert.False(next.IsCompleted);

        scenario.Enqueue(DescribeSemaphoreChanged(watchReqId, dataChanged: true, ownersChanged: false));

        Assert.True(await next.WaitAsync(TestTimeout));
        Assert.Equal("v2"u8.ToArray(), iterator.Current.Data);
    }

    [Fact]
    public async Task Recovery_AfterStreamClose_PreservesSessionIdAndProtectionKey()
    {
        var scenario = new MockCoordinationStream();
        scenario.AddStream(); // for recovery
        scenario.SessionId = 42;

        await using var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);
        Assert.Equal(42ul, session.SessionId);

        var firstStart = scenario.Written(0)
            .Single(r => r.RequestCase == SessionRequest.RequestOneofCase.SessionStart).SessionStart;

        // Break the first stream
        scenario.BreakStream(0);

        // Wait until the second stream has been opened and re-attached
        await WaitUntil(() => scenario.Written(1).Any(r =>
            r.RequestCase == SessionRequest.RequestOneofCase.SessionStart));

        var secondStart = scenario.Written(1)
            .Single(r => r.RequestCase == SessionRequest.RequestOneofCase.SessionStart).SessionStart;

        Assert.Equal(42ul, secondStart.SessionId);
        Assert.Equal(firstStart.ProtectionKey.ToByteArray(), secondStart.ProtectionKey.ToByteArray());
        Assert.Equal("/local/coord", secondStart.Path);
        Assert.True(secondStart.SeqNo > firstStart.SeqNo);
    }

    [Fact]
    public async Task Failure_FromServer_MarksSessionLost()
    {
        var scenario = new MockCoordinationStream();
        await using var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);

        scenario.Enqueue(Failure(StatusIds.Types.StatusCode.BadSession));

        await WaitUntil(() => session.SessionLostToken.IsCancellationRequested);
        Assert.True(session.SessionLostToken.IsCancellationRequested);

        await Assert.ThrowsAsync<YdbException>(() =>
            session.CreateSemaphoreAsync("x", 1).WaitAsync(TestTimeout));
    }

    [Fact]
    public async Task Dispose_SendsSessionStop()
    {
        var scenario = new MockCoordinationStream();
        var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);

        await session.DisposeAsync().AsTask().WaitAsync(TestTimeout);

        Assert.Contains(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.SessionStop);
    }

    [Fact]
    public async Task SendAfterDispose_Throws()
    {
        var scenario = new MockCoordinationStream();
        var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);
        await session.DisposeAsync().AsTask().WaitAsync(TestTimeout);

        await Assert.ThrowsAnyAsync<Exception>(() => session.CreateSemaphoreAsync("x", 1));
    }

    [Fact]
    public async Task ReplayPinned_AfterReconnect_ReSendsAcquireWithSameReqId()
    {
        var scenario = new MockCoordinationStream();
        scenario.AddStream();
        scenario.HandleRequest = _ => null; // we'll feed responses manually

        // Pre-stage SessionStarted via default infra; just no auto-reply to AcquireSemaphore.
        await using var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);

        var acquireTask = session.AcquireSemaphoreAsync("lock", timeout: null);

        // Wait for the first acquire to be written
        await WaitUntil(() => scenario.Written(0).Any(r =>
            r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore));

        var originalAcquire = scenario.Written(0)
            .Single(r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;

        // Break stream → worker reconnects
        scenario.BreakStream(0);

        await WaitUntil(() => scenario.Written(1).Any(r =>
            r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore));

        var replayedAcquire = scenario.Written(1)
            .Single(r => r.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore).AcquireSemaphore;

        Assert.Equal(originalAcquire.ReqId, replayedAcquire.ReqId);
        Assert.Equal(originalAcquire.Name, replayedAcquire.Name);

        // Finalize the acquire so the test can clean up
        scenario.Enqueue(AcquireSemaphoreResult(replayedAcquire.ReqId, acquired: true), streamIndex: 1);
        var lease = await acquireTask.WaitAsync(TestTimeout);
        Assert.NotNull(lease);
    }

    [Fact]
    public async Task Cancellation_DuringWait_CancelsPending()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = _ => null // never respond
        };

        await using var session = CreateSession(scenario);
        await session.WaitReadyAsync().WaitAsync(TestTimeout);

        using var cts = new CancellationTokenSource();
        var t = session.CreateSemaphoreAsync("never-ack", 1, cancellationToken: cts.Token);
        cts.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(() => t.WaitAsync(TestTimeout));
    }

    [Fact]
    public async Task Lease_DisposeAsync_ReleasesSemaphore()
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

        await using var session = CreateSession(scenario);

        await using (var lease = await session.AcquireSemaphoreAsync("lock").WaitAsync(TestTimeout))
        {
            Assert.NotNull(lease);
        }

        Assert.Contains(scenario.Written(),
            r => r.RequestCase == SessionRequest.RequestOneofCase.ReleaseSemaphore);
    }

    [Fact]
    public async Task Lease_LeaseLostToken_FiresOnSessionLost()
    {
        var scenario = new MockCoordinationStream
        {
            HandleRequest = r => r.AcquireSemaphore is null
                ? null
                : AcquireSemaphoreResult(r.AcquireSemaphore.ReqId, acquired: true)
        };

        await using var session = CreateSession(scenario);

        var lease = await session.AcquireSemaphoreAsync("lock").WaitAsync(TestTimeout);
        Assert.NotNull(lease);

        Assert.False(lease!.LeaseLostToken.IsCancellationRequested);

        scenario.Enqueue(Failure(StatusIds.Types.StatusCode.BadSession));

        await WaitUntil(() => lease.LeaseLostToken.IsCancellationRequested);
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static CoordinationSession CreateSession(MockCoordinationStream scenario)
        => new(scenario.SetupDriver().Object, "/local/coord", CoordinationSessionOptions.Default);

    private static async Task WaitUntil(Func<bool> predicate)
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        while (!predicate())
            await Task.Delay(10, cts.Token);
    }
}
