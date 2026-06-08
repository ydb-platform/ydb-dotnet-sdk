using Xunit;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Tests.Integration;

[Collection("CoordinationIntegration")]
public class DistributedLockIntegrationTests : IAsyncLifetime
{
    private const string NodePath = "test-coord-lock";
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(60);

    private CoordinationClient _admin = null!;

    public async Task InitializeAsync()
    {
        _admin = new CoordinationClient(Utils.ConnectionString);
        try { await _admin.DropNodeAsync(NodePath); }
        catch { /* node may not exist */ }
        await _admin.CreateNodeAsync(NodePath, new NodeConfig());
    }

    public async Task DisposeAsync()
    {
        try { await _admin.DropNodeAsync(NodePath); }
        catch { /* best effort */ }
        await _admin.DisposeAsync();
    }

    /// <summary>
    /// Distribution is emulated by spinning up several <see cref="CoordinationClient"/> instances
    /// (each with its own session) inside one process. They all race for the same lock name; the
    /// test asserts that at every moment in time at most one of them is inside the critical section.
    /// </summary>
    [Fact]
    public async Task ParallelAcquireRelease_GuaranteesMutualExclusion()
    {
        const string lockName = "mutex";
        const int workerCount = 6;
        const int iterationsPerWorker = 8;

        var currentlyInside = 0;
        var maxObservedInside = 0;
        var entriesObserved = 0;
        var maxLock = new object();

        var clients = Enumerable.Range(0, workerCount)
            .Select(_ => new CoordinationClient(Utils.ConnectionString))
            .ToArray();

        try
        {
            var workers = clients.Select((client, idx) => Task.Run(async () =>
            {
                for (var i = 0; i < iterationsPerWorker; i++)
                {
                    await using var locker = await client
                        .AcquireLockAsync(NodePath, lockName, BitConverter.GetBytes(idx));

                    var inside = Interlocked.Increment(ref currentlyInside);
                    try
                    {
                        Assert.Equal(1, inside);
                        lock (maxLock) maxObservedInside = Math.Max(maxObservedInside, inside);
                        Interlocked.Increment(ref entriesObserved);

                        // Hold the lock briefly so that any breach of mutual exclusion has a chance
                        // to surface as a concurrent entry.
                        await Task.Delay(15);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref currentlyInside);
                    }
                }
            })).ToArray();

            await Task.WhenAll(workers).WaitAsync(TestTimeout);

            Assert.Equal(workerCount * iterationsPerWorker, entriesObserved);
            Assert.Equal(1, maxObservedInside);
        }
        finally
        {
            foreach (var c in clients) await c.DisposeAsync();
        }
    }

    [Fact]
    public async Task TryAcquire_WhenLockHeld_ReturnsNull()
    {
        const string lockName = "try-busy";
        await using var holderClient = new CoordinationClient(Utils.ConnectionString);
        await using var contenderClient = new CoordinationClient(Utils.ConnectionString);

        await using var holder = await holderClient
            .AcquireLockAsync(NodePath, lockName)
            .WaitAsync(TestTimeout);

        var contender = await contenderClient
            .TryAcquireLockAsync(NodePath, lockName)
            .WaitAsync(TestTimeout);

        Assert.Null(contender);
    }

    [Fact]
    public async Task LockLostToken_IsCancelled_AfterDispose()
    {
        const string lockName = "lost-token";
        var client = new CoordinationClient(Utils.ConnectionString);
        try
        {
            var locker = await client
                .AcquireLockAsync(NodePath, lockName)
                .WaitAsync(TestTimeout);

            Assert.False(locker.LockLostToken.IsCancellationRequested);

            await locker.DisposeAsync().AsTask().WaitAsync(TestTimeout);

            Assert.True(locker.LockLostToken.IsCancellationRequested);
        }
        finally
        {
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task ReleasedLock_CanBeReacquired_ByNextCandidate()
    {
        const string lockName = "handoff";
        await using var clientA = new CoordinationClient(Utils.ConnectionString);
        await using var clientB = new CoordinationClient(Utils.ConnectionString);

        var lockA = await clientA.AcquireLockAsync(NodePath, lockName).WaitAsync(TestTimeout);

        var bAcquire = clientB.AcquireLockAsync(NodePath, lockName);
        await Task.Delay(300);
        Assert.False(bAcquire.IsCompleted);

        await lockA.DisposeAsync();

        var lockB = await bAcquire.WaitAsync(TestTimeout);
        await lockB.DisposeAsync();
    }
}

[CollectionDefinition("CoordinationIntegration", DisableParallelization = true)]
public sealed class CoordinationIntegrationCollection
{
    // Disables xUnit parallelism for integration tests that mutate shared coordination nodes.
}
