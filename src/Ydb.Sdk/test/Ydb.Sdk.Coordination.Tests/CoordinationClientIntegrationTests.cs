using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using ConsistencyMode = Ydb.Sdk.Coordination.Description.ConsistencyMode;
using RateLimiterCountersMode = Ydb.Sdk.Coordination.Description.RateLimiterCountersMode;

namespace Ydb.Sdk.Coordination.Tests;

public class CoordinationClientIntegrationTests
{
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);
    private readonly ITestOutputHelper _output;

    public CoordinationClientIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    //  Given, When, Then
    [Fact]
    public async Task CreateNode()
    {
        _output.WriteLine("=== START CreateNode test ===");
        //  Given
        var coordinationNodeSettings = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };

        var oldNodeConfig = coordinationNodeSettings.ToProto();
        var pathNode = "local/test1";
        _output.WriteLine($"Creating node at path: {pathNode}");
        _output.WriteLine("Old Config:");
        _output.WriteLine($"  SelfCheckPeriodMillis: {oldNodeConfig.SelfCheckPeriodMillis}");
        _output.WriteLine($"  SessionGracePeriodMillis: {oldNodeConfig.SessionGracePeriodMillis}");
        _output.WriteLine($"  ReadConsistencyMode: {oldNodeConfig.ReadConsistencyMode}");
        _output.WriteLine($"  AttachConsistencyMode: {oldNodeConfig.AttachConsistencyMode}");
        _output.WriteLine($"  RateLimiterCountersMode: {oldNodeConfig.RateLimiterCountersMode}");

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var describeNode = await _coordinationClient.DescribeNode(pathNode);
        var describeNodeConfig = describeNode.ToProto();
        _output.WriteLine("MyValidate: " + _coordinationClient.MyValidate(pathNode));
        _output.WriteLine("YdbValidate: " + _coordinationClient.YdbValidate(pathNode));
        _output.WriteLine("YdbValidate2: " + _coordinationClient.YdbValidate2(pathNode));
        _output.WriteLine("New Config:");
        _output.WriteLine($"  SelfCheckPeriodMillis: {describeNodeConfig.SelfCheckPeriodMillis}");
        _output.WriteLine($"  SessionGracePeriodMillis: {describeNodeConfig.SessionGracePeriodMillis}");
        _output.WriteLine($"  ReadConsistencyMode: {describeNodeConfig.ReadConsistencyMode}");
        _output.WriteLine($"  AttachConsistencyMode: {describeNodeConfig.AttachConsistencyMode}");
        _output.WriteLine($"  RateLimiterCountersMode: {describeNodeConfig.RateLimiterCountersMode}");

        //Then
        Assert.Equal(oldNodeConfig.SelfCheckPeriodMillis, describeNodeConfig.SelfCheckPeriodMillis);
        Assert.Equal(oldNodeConfig.SessionGracePeriodMillis, describeNodeConfig.SessionGracePeriodMillis);
        Assert.Equal(oldNodeConfig.ReadConsistencyMode, describeNodeConfig.ReadConsistencyMode);
        Assert.Equal(oldNodeConfig.AttachConsistencyMode, describeNodeConfig.AttachConsistencyMode);
        Assert.Equal(oldNodeConfig.RateLimiterCountersMode, describeNodeConfig.RateLimiterCountersMode);
        await _coordinationClient.DropNode(pathNode);

        _output.WriteLine("Assertions passed.");
    }

    [Fact]
    public async Task DoubleCreateNode()
    {
        _output.WriteLine("=== START DoubleCreateNode test ===");
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var alterCoordinationNode = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(10),
            SessionGracePeriod = TimeSpan.FromSeconds(20),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Aggregated
        };
        var pathNode = "/local/doubleCreateNode";

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var oldDescribeNode = await _coordinationClient.DescribeNode(pathNode);
        var oldNodeConfig = oldDescribeNode.ToProto();
        // We can send other config to create node, but it won't be applied
        await _coordinationClient.CreateNode(pathNode, alterCoordinationNode);
        var describeNode = await _coordinationClient.DescribeNode(pathNode);
        var describeNodeConfig = describeNode.ToProto();

        //Then
        await _coordinationClient.DropNode(pathNode);
        Assert.Equal(oldNodeConfig.SelfCheckPeriodMillis, describeNodeConfig.SelfCheckPeriodMillis);
        Assert.Equal(oldNodeConfig.SessionGracePeriodMillis, describeNodeConfig.SessionGracePeriodMillis);
        Assert.Equal(oldNodeConfig.ReadConsistencyMode, describeNodeConfig.ReadConsistencyMode);
        Assert.Equal(oldNodeConfig.AttachConsistencyMode, describeNodeConfig.AttachConsistencyMode);
        Assert.Equal(oldNodeConfig.RateLimiterCountersMode, describeNodeConfig.RateLimiterCountersMode);

        _output.WriteLine("Assertions passed.");
    }

    [Fact]
    public async Task DoubleDropNode()
    {
        _output.WriteLine("=== START DoubleDropNode test ===");
        //  Given
        var config = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "local/doubleDropNode";

        // When
        await _coordinationClient.CreateNode(pathNode, config);
        await _coordinationClient.DropNode(pathNode);

        var exception = await Assert.ThrowsAsync<YdbException>(async () =>
        {
            await _coordinationClient.DropNode(pathNode);
        });
        _output.WriteLine(exception.Message);
        //Then
        Assert.Equal("Drop node failed [200200] Error: Path does not exist\r\n", exception.Message);
        _output.WriteLine("Assertions passed.");
    }

    [Fact]
    public async Task DescribeNonExistentNode()
    {
        _output.WriteLine("=== START DescribeNonExistentNode test ===");
        //  Given
        var pathNode = "/local/test1";

        // When
        var exception = await Assert.ThrowsAsync<YdbException>(async () =>
        {
            await _coordinationClient.DescribeNode(pathNode);
        });

        //Then
        Assert.Equal("Describe node failed ", exception.Message);
        _output.WriteLine("Assertions passed.");
    }

    [Fact]
    public async Task AlterNode()
    {
        _output.WriteLine("=== START AlterNode test ===");
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(5),
            SessionGracePeriod = TimeSpan.FromSeconds(25),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var alterCoordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(5),
            SessionGracePeriod = TimeSpan.FromSeconds(25),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var initialConfig = coordinationNodeConfig.ToProto();
        var alterNodeConfig = alterCoordinationNodeConfig.ToProto();
        var pathNode = "/local/test2";

        _output.WriteLine($"Node path: {pathNode}");

        _output.WriteLine("Initial Config:");
        _output.WriteLine($"  SelfCheckPeriodMillis: {initialConfig.SelfCheckPeriodMillis}");
        _output.WriteLine($"  SessionGracePeriodMillis: {initialConfig.SessionGracePeriodMillis}");
        _output.WriteLine($"  ReadConsistencyMode: {initialConfig.ReadConsistencyMode}");
        _output.WriteLine($"  AttachConsistencyMode: {initialConfig.AttachConsistencyMode}");
        _output.WriteLine($"  RateLimiterCountersMode: {initialConfig.RateLimiterCountersMode}");

        _output.WriteLine("Alter Config:");
        _output.WriteLine($"  SelfCheckPeriodMillis: {alterNodeConfig.SelfCheckPeriodMillis}");
        _output.WriteLine($"  SessionGracePeriodMillis: {alterNodeConfig.SessionGracePeriodMillis}");
        _output.WriteLine($"  ReadConsistencyMode: {alterNodeConfig.ReadConsistencyMode}");
        _output.WriteLine($"  AttachConsistencyMode: {alterNodeConfig.AttachConsistencyMode}");
        _output.WriteLine($"  RateLimiterCountersMode: {alterNodeConfig.RateLimiterCountersMode}");


        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        await _coordinationClient.AlterNode(pathNode, alterCoordinationNodeConfig);

        var describeNode = await _coordinationClient.DescribeNode(pathNode);
        var describeNodeConfig = describeNode.ToProto();

        _output.WriteLine("Result Config:");
        _output.WriteLine($"  SelfCheckPeriodMillis: {describeNodeConfig.SelfCheckPeriodMillis}");
        _output.WriteLine($"  SessionGracePeriodMillis: {describeNodeConfig.SessionGracePeriodMillis}");
        _output.WriteLine($"  ReadConsistencyMode: {describeNodeConfig.ReadConsistencyMode}");
        _output.WriteLine($"  AttachConsistencyMode: {describeNodeConfig.AttachConsistencyMode}");
        _output.WriteLine($"  RateLimiterCountersMode: {describeNodeConfig.RateLimiterCountersMode}");


        //Then
        Assert.Equal(alterNodeConfig.SelfCheckPeriodMillis, describeNodeConfig.SelfCheckPeriodMillis);
        Assert.Equal(alterNodeConfig.SessionGracePeriodMillis, describeNodeConfig.SessionGracePeriodMillis);
        Assert.Equal(alterNodeConfig.ReadConsistencyMode, describeNodeConfig.ReadConsistencyMode);
        Assert.Equal(alterNodeConfig.AttachConsistencyMode, describeNodeConfig.AttachConsistencyMode);
        Assert.Equal(alterNodeConfig.RateLimiterCountersMode, describeNodeConfig.RateLimiterCountersMode);
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task CreateSession()
    {
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test3";

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSessionq = _coordinationClient.CreateSession(pathNode);
        await using var coordinationSessionw = _coordinationClient.CreateSession(pathNode);
        var stateSession1 = coordinationSessionq.Status();
        //Then
        Assert.Equal(StateSession.Connecting, stateSession1);
        coordinationSessionq.Status();
        await coordinationSessionq.Close();
        Assert.Equal(StateSession.Closed, coordinationSessionq.Status());
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task CreateSemaphore()
    {
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test4";
        var semaphoreName = "semaphore1";
        byte[] semaphoreData = [0x00, 0x12];
        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSession = _coordinationClient.CreateSession(pathNode);
        var semaphore = coordinationSession.Semaphore(semaphoreName);
        await semaphore.Create(10, semaphoreData);
        var task = await semaphore.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);
        _output.WriteLine("Describe Result:");
        _output.WriteLine($"  Name: {task.Name}");
        _output.WriteLine($"  Limit: {task.Limit}");
        _output.WriteLine($"  Count: {task.Count}");
        _output.WriteLine($"  Ephemeral: {task.Ephemeral}");
        _output.WriteLine($"  Data: {task.Data}");
        _output.WriteLine($"  OwnersList count: {task.OwnersList}");
        _output.WriteLine($"  WaitersList count: {task.WaitersList}");
        //Then
        await semaphore.Delete(false);
        await coordinationSession.Close();
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task DescribeAndUpdateSemaphore()
    {
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test5";
        var semaphoreName = "semaphore2";
        byte[] semaphoreData1 = [0x00, 0x12];
        byte[] semaphoreData2 = [0x01, 0x02, 0x03];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var coordinationSession2 = _coordinationClient.CreateSession(pathNode);
        // When
        var semaphore1 = coordinationSession1.Semaphore(semaphoreName);
        await semaphore1.Create(10, semaphoreData1);
        var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
        var describeBefore = await semaphore2.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);
        await semaphore1.Update(semaphoreData2);
        var describeAfter = await semaphore2.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);
        // ---- OUTPUT BEFORE ----
        _output.WriteLine("Describe BEFORE:");
        _output.WriteLine($"  Name: {describeBefore.Name}");
        _output.WriteLine($"  Limit: {describeBefore.Limit}");
        _output.WriteLine($"  Count: {describeBefore.Count}");
        _output.WriteLine($"  Ephemeral: {describeBefore.Ephemeral}");
        _output.WriteLine(
            $"  Data: {describeBefore.Data}");
        _output.WriteLine($"  OwnersList count: {describeBefore.OwnersList}");
        _output.WriteLine($"  WaitersList count: {describeBefore.WaitersList}");
        // ---- OUTPUT AFTER ----
        _output.WriteLine("Describe AFTER:");
        _output.WriteLine($"  Name: {describeAfter.Name}");
        _output.WriteLine($"  Limit: {describeAfter.Limit}");
        _output.WriteLine($"  Count: {describeAfter.Count}");
        _output.WriteLine($"  Ephemeral: {describeAfter.Ephemeral}");
        _output.WriteLine(
            $"  Data: {describeAfter.Data}");
        _output.WriteLine($"  OwnersList count: {describeAfter.OwnersList}");
        _output.WriteLine($"  WaitersList count: {describeAfter.WaitersList}");
        //Then
        // ---- Assert BEFORE ----
        Assert.Equal(semaphoreName, describeBefore.Name);
        Assert.Equal((ulong)10, describeBefore.Limit);
        Assert.Equal((ulong)0, describeBefore.Count);
        Assert.False(describeBefore.Ephemeral);
        Assert.NotNull(describeBefore.Data);
        Assert.Equal(semaphoreData1, describeBefore.Data);
        Assert.Empty(describeBefore.OwnersList);
        Assert.Empty(describeBefore.WaitersList);
        // ---- Assert AFTER ----
        Assert.Equal(semaphoreName, describeAfter.Name);
        Assert.Equal((ulong)10, describeAfter.Limit);
        Assert.Equal((ulong)0, describeAfter.Count);
        Assert.NotNull(describeAfter.Data);
        Assert.Equal(semaphoreData2, describeAfter.Data);
        Assert.Empty(describeAfter.OwnersList);
        Assert.Empty(describeAfter.WaitersList);
        await semaphore2.Delete(false);
        await coordinationSession1.Close();
        await coordinationSession2.Close();
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task AcquireSemaphore()
    {
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test6";
        var semaphoreName = "semaphore3";
        byte[] semaphoreData1 = [0x00, 0x12];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var coordinationSession2 = _coordinationClient.CreateSession(pathNode);
        var semaphore1 = coordinationSession1.Semaphore(semaphoreName);
        var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
        await semaphore1.Create(20, semaphoreData1);
        // When
        var lease = await semaphore1.Acquire(15, false, null, TimeSpan.FromSeconds(5));
        var exception = await Assert.ThrowsAsync<YdbException>(async () =>
        {
            await semaphore2.Acquire(15, false, null, TimeSpan.FromSeconds(5));
        });
        //Then
        _output.WriteLine($"Waiting for semaphore {exception.Message}");
        Assert.Equal("Acquire semaphore failed", exception.Message);
        await lease.Release();
        await semaphore1.Delete(false);
        await coordinationSession1.Close();
        await coordinationSession2.Close();
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task AcquireEphemeralSemaphore()
    {
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Strict,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test7";
        var semaphoreName = "semaphore3_2";
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var semaphore = coordinationSession1.Semaphore(semaphoreName);
        // When
        var lease = await semaphore.Acquire(ulong.MaxValue, true, null, null);
        await lease.Release();
        //Then
        await coordinationSession1.Close();
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task TryAcquireSemaphore()
    {
        // Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test_try";
        var semaphoreName = "semaphore_try";
        byte[] semaphoreData1 = [0x00, 0x12];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var coordinationSession2 = _coordinationClient.CreateSession(pathNode);
        var semaphore1 = coordinationSession1.Semaphore(semaphoreName);
        var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
        await semaphore1.Create(10, semaphoreData1);
        // When
        var lease1 = await semaphore1.TryAcquire(10, false, null);
        var lease2 = await semaphore2.TryAcquire(5, false, null);
        // Then
        Assert.NotNull(lease1);
        Assert.Null(lease2);
        await lease1.Release();
        await semaphore1.Delete(false);
        await coordinationSession1.Close();
        await _coordinationClient.DropNode(pathNode);
    }

    [Fact]
    public async Task WatchSemaphore()
    {
        //  Given
        var coordinationNodeConfig = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        var pathNode = "/local/test8";
        var semaphoreName = "semaphore4";
        byte[] semaphoreData1 = [0x00, 0x12];
        byte[] semaphoreData2 = [0x01, 0x02, 0x03];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeConfig);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var coordinationSession2 = _coordinationClient.CreateSession(pathNode);
        var semaphore1 = coordinationSession1.Semaphore(semaphoreName);
        var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
        // When
        await semaphore1.Create(20, semaphoreData1);
        // --- WATCH ---
        var watch = await semaphore2.WatchSemaphore(DescribeSemaphoreMode.WithOwners, WatchSemaphoreMode.WatchOwners);
        var initial = watch.Initial;
        Assert.Empty(initial.OwnersList);
        var updates = watch.Updates.GetAsyncEnumerator();
        var moveTask = updates.MoveNextAsync();
        Assert.False(moveTask.IsCompleted);
        // --- ACQUIRE ---
        await semaphore1.Acquire(15, false, null, TimeSpan.FromSeconds(5));
        Assert.True(await moveTask);
        var afterAcquire = updates.Current;
        Assert.Single(afterAcquire.OwnersList);
        // --- REWATCH (data) ---
        await updates.DisposeAsync();
        watch = await semaphore2.WatchSemaphore(
            DescribeSemaphoreMode.WithOwnersAndWaiters,
            WatchSemaphoreMode.WatchData);
        updates = watch.Updates.GetAsyncEnumerator();
        var moveTask2 = updates.MoveNextAsync();
        Assert.False(moveTask2.IsCompleted);
        // --- UPDATE DATA ---
        await semaphore1.Update(semaphoreData2);
        Assert.True(await moveTask2);
        var afterUpdate = updates.Current;
        Assert.Equal(semaphoreData2, afterUpdate.Data);
        // --- REWATCH (all) ---
        watch = await semaphore2.WatchSemaphore(
            DescribeSemaphoreMode.DataOnly,
            WatchSemaphoreMode.WatchDataAndOwners);
        updates = watch.Updates.GetAsyncEnumerator();
        var moveTask3 = updates.MoveNextAsync();
        Assert.False(moveTask3.IsCompleted);
        // --- RELEASE ---
        await semaphore1.Release();
        Assert.True(await moveTask3);
        var afterRelease = updates.Current;
        Assert.Empty(afterRelease.OwnersList);
        // --- DESCRIBE ---
        var final = await semaphore2.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);
        Assert.Equal(semaphoreData2, final.Data);
        Assert.Empty(final.OwnersList);
        await updates.DisposeAsync();
        //Then
        Assert.Equal(semaphoreName, initial.Name);
        Assert.Equal((ulong)0, initial.Count);
        Assert.Equal((ulong)20, initial.Limit);
        //wait lease1.Release();
        await updates.DisposeAsync();
        await semaphore1.Release();
        await semaphore1.Delete(false);
        await coordinationSession1.Close();
        await coordinationSession2.Close();
        await _coordinationClient.DropNode(pathNode);
    }
}
