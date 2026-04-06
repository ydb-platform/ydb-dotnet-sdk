using Xunit;
using Xunit.Abstractions;
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
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var describeCoordinationNodeSettings = new DescribeCoordinationNodeSettings();
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var oldNodeConfig = coordinationNodeSettings.Config.ToProto();
        var pathNode = "/local/test";

        _output.WriteLine($"Creating node at path: {pathNode}");
        _output.WriteLine("Old Config:");
        _output.WriteLine($"  SelfCheckPeriodMillis: {oldNodeConfig.SelfCheckPeriodMillis}");
        _output.WriteLine($"  SessionGracePeriodMillis: {oldNodeConfig.SessionGracePeriodMillis}");
        _output.WriteLine($"  ReadConsistencyMode: {oldNodeConfig.ReadConsistencyMode}");
        _output.WriteLine($"  AttachConsistencyMode: {oldNodeConfig.AttachConsistencyMode}");
        _output.WriteLine($"  RateLimiterCountersMode: {oldNodeConfig.RateLimiterCountersMode}");

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var describeNode = await _coordinationClient.DescribeNode(pathNode, describeCoordinationNodeSettings);
        var describeNodeConfig = describeNode.ToProto();
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
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);

        _output.WriteLine("Assertions passed.");
    }


    [Fact]
    public async Task AlterNode()
    {
        _output.WriteLine("=== START AlterNode test ===");
        //  Given
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(25))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var alterCoordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Aggregated)
        };
        var describeCoordinationNodeSettings = new DescribeCoordinationNodeSettings();
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var initialConfig = coordinationNodeSettings.Config.ToProto();
        var alterNodeConfig = alterCoordinationNodeSettings.Config.ToProto();
        var pathNode = "/local/test";

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
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        await _coordinationClient.AlterNode(pathNode, alterCoordinationNodeSettings);

        var describeNode = await _coordinationClient.DescribeNode(pathNode, describeCoordinationNodeSettings);
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
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
    }

    [Fact]
    public async Task CreateSession()
    {
        //  Given
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var pathNode = "/local/test";

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var coordinationSession = _coordinationClient.CreateSession(pathNode);

        //Then
        coordinationSession.Status();
        coordinationSession.Close();
        coordinationSession.Status();
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
    }

    [Fact]
    public async Task CreateSemaphore()
    {
        //  Given
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var pathNode = "/local/test";
        var semaphoreName = "semaphore1";
        byte[] semaphoreData = [0x00, 0x12];


        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var coordinationSession = _coordinationClient.CreateSession(pathNode);
        var semaphore = coordinationSession.Semaphore(semaphoreName);
        await semaphore.Create(10, semaphoreData);

        var task = await semaphore.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);
        _output.WriteLine("Describe Result:");
        _output.WriteLine($"  Name: {task.Name}");
        _output.WriteLine($"  Limit: {task.Limit}");
        _output.WriteLine($"  Count: {task.Count}");
        _output.WriteLine($"  Ephemeral: {task.Ephemeral}");
        _output.WriteLine($"  Data: {(task.Data != null ? BitConverter.ToString(task.Data.ToByteArray()) : "null")}");
        _output.WriteLine($"  Owners count: {task.Owners?.Count ?? 0}");
        _output.WriteLine($"  Waiters count: {task.Waiters?.Count ?? 0}");

        //Then
        await semaphore.Delete(false);
        await coordinationSession.Close();
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
    }

    [Fact]
    public async Task DescribeAndUpdateSemaphore()
    {
        //  Given
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var pathNode = "/local/test";
        var semaphoreName = "semaphore2";
        byte[] semaphoreData1 = [0x00, 0x12];
        byte[] semaphoreData2 = [0x01, 0x02, 0x03];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
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
            $"  Data: {(describeBefore.Data != null ? BitConverter.ToString(describeBefore.Data.ToByteArray()) : "null")}");
        _output.WriteLine($"  Owners count: {describeBefore.Owners?.Count ?? 0}");
        _output.WriteLine($"  Waiters count: {describeBefore.Waiters?.Count ?? 0}");

        // ---- OUTPUT AFTER ----
        _output.WriteLine("Describe AFTER:");
        _output.WriteLine($"  Name: {describeAfter.Name}");
        _output.WriteLine($"  Limit: {describeAfter.Limit}");
        _output.WriteLine($"  Count: {describeAfter.Count}");
        _output.WriteLine($"  Ephemeral: {describeAfter.Ephemeral}");
        _output.WriteLine(
            $"  Data: {(describeAfter.Data != null ? BitConverter.ToString(describeAfter.Data.ToByteArray()) : "null")}");
        _output.WriteLine($"  Owners count: {describeAfter.Owners?.Count ?? 0}");
        _output.WriteLine($"  Waiters count: {describeAfter.Waiters?.Count ?? 0}");

        //Then
        // ---- Assert BEFORE ----

        Assert.Equal(semaphoreName, describeBefore.Name);
        Assert.Equal((ulong)10, describeBefore.Limit);
        Assert.Equal((ulong)0, describeBefore.Count);
        Assert.False(describeBefore.Ephemeral);

        Assert.NotNull(describeBefore.Data);
        Assert.Equal(semaphoreData1, describeBefore.Data.ToByteArray());

        Assert.Empty(describeBefore.Owners);
        Assert.Empty(describeBefore.Waiters);
        // ---- Assert AFTER ----

        Assert.Equal(semaphoreName, describeAfter.Name);
        Assert.Equal((ulong)10, describeAfter.Limit); // limit должен остаться
        Assert.Equal((ulong)0, describeAfter.Count);

        Assert.NotNull(describeAfter.Data);
        Assert.Equal(semaphoreData2, describeAfter.Data.ToByteArray());

        Assert.Empty(describeAfter.Owners);
        Assert.Empty(describeAfter.Waiters);
        await semaphore2.Delete(false);
        await coordinationSession1.Close();
        await coordinationSession2.Close();
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
    }

    [Fact]
    public async Task AcquireSemaphore()
    {
        //  Given
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var pathNode = "/local/test";
        var semaphoreName = "semaphore3";
        byte[] semaphoreData1 = [0x00, 0x12];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var semaphore = coordinationSession1.Semaphore(semaphoreName);
        await semaphore.Create(20, semaphoreData1);
        // When
        var lease = await semaphore.Acquire(15, false, null, TimeSpan.FromSeconds(5));
        await lease.Release();
        //Then
        await semaphore.Delete(false);
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
        await coordinationSession1.Close();

        /*
       var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
       // When
       var lease2 = await semaphore2.Acquire(15, false, null, TimeSpan.FromSeconds(5));
       await semaphore2.Acquire(15, false, null, TimeSpan.FromSeconds(5));
       await lease2.Release();
       await lease2.Release();


       Lease lease1;
       var exception = await Assert.ThrowsAsync<YdbException>(async () =>
       {
           // Попытка повторного захвата семафора
           lease1 = await semaphore1.Acquire(15, false, null, TimeSpan.FromSeconds(5));
       });

       await lease2.Release();

       //Then
       Assert.Equal("Acquire semaphore failed", exception.Message);
       */
    }

    [Fact]
    public async Task WatchSemaphore()
    {
        //  Given
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var pathNode = "/local/test";
        var semaphoreName = "semaphore4";
        byte[] semaphoreData1 = [0x00, 0x12];
        byte[] semaphoreData2 = [0x01, 0x02, 0x03];
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var coordinationSession1 = _coordinationClient.CreateSession(pathNode);
        var coordinationSession2 = _coordinationClient.CreateSession(pathNode);
        var semaphore1 = coordinationSession1.Semaphore(semaphoreName);
        var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
        // When
        await semaphore1.Create(20, semaphoreData1);
        // --- WATCH #1 ---
        var watch = await semaphore2.WatchSemaphore(DescribeSemaphoreMode.WithOwners, WatchSemaphoreMode.WatchOwners);
        var initial = watch.Initial;
        Assert.Empty(initial.Owners);
        var updates = watch.Updates.GetAsyncEnumerator();
        var moveTask = updates.MoveNextAsync();
        Assert.False(moveTask.IsCompleted);
        // --- ACQUIRE ---
         await semaphore1.Acquire(15, false, null, TimeSpan.FromSeconds(5));
        Assert.True(await moveTask);
        var afterAcquire = updates.Current;
        Assert.Single(afterAcquire.Owners);
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
        await updates.DisposeAsync();
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
        Assert.Empty(afterRelease.Owners);
        
        // --- DESCRIBE ---
        var final = await semaphore2.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);

        Assert.Equal(semaphoreData2, final.Data);
        Assert.Empty(final.Owners);

        await updates.DisposeAsync();
        /*
        var moveTask1 = enumerator.MoveNextAsync();
        var semaphoreDescription1 = enumerator.Current;
        Assert.False(moveTask1.IsCompleted);
        /*
        var lease1 = await semaphore1.Acquire(15, false, null, TimeSpan.FromSeconds(5));
        Assert.True(await moveTask1);

        var description2 = enumerator.Current;
       // Assert.Single(description2.GetOwnersList());
        */

        //Then
        Assert.Equal(semaphoreName, initial.Name);
        Assert.Equal((ulong)0, initial.Count);
        Assert.Equal((ulong)20, initial.Limit);

        //wait lease1.Release();
        await updates.DisposeAsync();
        await semaphore1.Release();
        await semaphore1.Delete(false);

        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
        await coordinationSession1.Close();
        await coordinationSession2.Close();
        /*
       var semaphore2 = coordinationSession2.Semaphore(semaphoreName);
       // When
       var lease2 = await semaphore2.Acquire(15, false, null, TimeSpan.FromSeconds(5));
       await semaphore2.Acquire(15, false, null, TimeSpan.FromSeconds(5));
       await lease2.Release();
       await lease2.Release();


       Lease lease1;
       var exception = await Assert.ThrowsAsync<YdbException>(async () =>
       {
           // Попытка повторного захвата семафора
           lease1 = await semaphore1.Acquire(15, false, null, TimeSpan.FromSeconds(5));
       });

       await lease2.Release();

       //Then
       Assert.Equal("Acquire semaphore failed", exception.Message);
       */
    }

    /*
     *    public void watchSemaphoreTest() {
        String nodePath = "test-sessions/watch-semaphore-test";
        String semaphoreName = "semaphore4";
        Duration timeout = Duration.ofSeconds(5);
        byte[] updateData = new byte[] { 0x01, 0x02, 0x03 };

        logger.info("create node");
        CLIENT.createNode(nodePath).join().expectSuccess("creating of node failed");
        logger.info("create sessions");
        CoordinationSession session1 = CLIENT.createSession(nodePath);
        CoordinationSession session2 = CLIENT.createSession(nodePath);
        logger.info("connect sessions");
        session1.connect().join().expectSuccess("cannot connect session");
        session2.connect().join().expectSuccess("cannot connect session");

        logger.info("create semaphore");
        session1.createSemaphore(semaphoreName, 20).join().expectSuccess("cannot create semaphore");

        logger.info("watch semaphore");
        SemaphoreWatcher watcher = session2.watchSemaphore(semaphoreName,
                DescribeSemaphoreMode.WITH_OWNERS, WatchSemaphoreMode.WATCH_OWNERS
        ).join().getValue();

        Assert.assertEquals(semaphoreName, watcher.getDescription().getName());
        Assert.assertEquals(0, watcher.getDescription().getData().length);
        Assert.assertEquals(20, watcher.getDescription().getLimit());
        Assert.assertTrue(watcher.getDescription().getOwnersList().isEmpty());
        Assert.assertTrue(watcher.getDescription().getWaitersList().isEmpty());

        Assert.assertFalse(watcher.getChangedFuture().isDone());

        logger.info("take lease");
        CompletableFuture<Result<SemaphoreLease>> lease1 = session1.acquireSemaphore(semaphoreName, 15, timeout);
        lease1.join().getStatus().expectSuccess("cannot acquire semaphore");

        logger.info("rewatch semaphore");
        SemaphoreChangedEvent changed = watcher.getChangedFuture().join().getValue();
        Assert.assertFalse(changed.isDataChanged());
        Assert.assertTrue(changed.isOwnersChanged());

        watcher = session2.watchSemaphore(semaphoreName,
                DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS, WatchSemaphoreMode.WATCH_DATA
        ).join().getValue();

        Assert.assertEquals(semaphoreName, watcher.getDescription().getName());
        Assert.assertEquals(0, watcher.getDescription().getData().length);
        Assert.assertEquals(20, watcher.getDescription().getLimit());
        Assert.assertEquals(1, watcher.getDescription().getOwnersList().size());
        Assert.assertEquals(session1.getId(), watcher.getDescription().getOwnersList().get(0).getId());
        Assert.assertTrue(watcher.getDescription().getWaitersList().isEmpty());

        Assert.assertFalse(watcher.getChangedFuture().isDone());

        logger.info("describe updated semaphore");
        session1.updateSemaphore(semaphoreName, updateData).join().expectSuccess("cannot update semaphore");

        logger.info("rewatch semaphore");
        changed = watcher.getChangedFuture().join().getValue();
        Assert.assertTrue(changed.isDataChanged());
        Assert.assertFalse(changed.isOwnersChanged());

        watcher = session2.watchSemaphore(semaphoreName,
                DescribeSemaphoreMode.DATA_ONLY, WatchSemaphoreMode.WATCH_DATA_AND_OWNERS
        ).join().getValue();

        Assert.assertEquals(semaphoreName, watcher.getDescription().getName());
        Assert.assertArrayEquals(updateData, watcher.getDescription().getData());
        Assert.assertEquals(20, watcher.getDescription().getLimit());
        Assert.assertTrue(watcher.getDescription().getOwnersList().isEmpty());
        Assert.assertTrue(watcher.getDescription().getWaitersList().isEmpty());

        Assert.assertFalse(watcher.getChangedFuture().isDone());

        logger.info("release lease");
        lease1.join().getValue().release().join();

        logger.info("rewatch semaphore");
        changed = watcher.getChangedFuture().join().getValue();
        Assert.assertFalse(changed.isDataChanged());
        Assert.assertTrue(changed.isOwnersChanged());

        SemaphoreDescription description = session2.describeSemaphore(semaphoreName,
                DescribeSemaphoreMode.WITH_OWNERS_AND_WAITERS).join().getValue();

        Assert.assertEquals(semaphoreName, description.getName());
        Assert.assertArrayEquals(updateData, description.getData());
        Assert.assertEquals(20, description.getLimit());
        Assert.assertTrue(description.getOwnersList().isEmpty());
        Assert.assertTrue(description.getWaitersList().isEmpty());

        logger.info("delete semaphore");
        session2.deleteSemaphore(semaphoreName).join().expectSuccess("cannot create semaphore");
        logger.info("stop sessions");
        session1.close();
        session2.close();
        logger.info("drop node");
        CLIENT.dropNode(nodePath).join().expectSuccess("removing of node failed");
    }
     */

    /*
     *
     * [Fact]
public async Task AcquireSemaphoreTest()
{
    var nodePath = $"test-sessions/acquire-semaphore-test-{Guid.NewGuid()}";
    var semaphoreName = "semaphore2";
    var timeout = TimeSpan.FromSeconds(5);

    // create node
    await _coordinationClient.CreateNode(nodePath);

    // create sessions
    var session1 = _coordinationClient.CreateSession(nodePath);
    var session2 = _coordinationClient.CreateSession(nodePath);

    // connect sessions (если есть такой метод)
    await session1.Connect();
    await session2.Connect();

    try
    {
        // create semaphore
        var semaphore1 = session1.Semaphore(semaphoreName);
        await semaphore1.Create(20, null);

        // take first lease
        var semaphore2 = session2.Semaphore(semaphoreName);
        var lease2Task = semaphore2.Acquire(15, ephemeral: false, data: null, timeout);

        var lease2 = await lease2Task; // должен успешно выполниться

        // request second lease, waiting
        var lease1Task = semaphore1.Acquire(15, ephemeral: false, data: null, timeout);

        Assert.False(lease1Task.IsCompleted); // аналог isDone()

        // release first lease → второй должен завершиться
        await lease2.Release();

        var lease1 = await lease1Task; // теперь должен завершиться

        // release second lease
        await lease1.Release();

        // delete semaphore
        await semaphore2.Delete(false);

        // take first ephemeral lease
        lease2Task = semaphore2.Acquire(1, ephemeral: true, data: null, timeout);
        lease2 = await lease2Task;

        // request second ephemeral lease, waiting
        lease1Task = semaphore1.Acquire(1, ephemeral: true, data: null, timeout);

        Assert.False(lease1Task.IsCompleted);

        // release first → второй завершается
        await lease2.Release();

        lease1 = await lease1Task;

        // release second
        await lease1.Release();
    }
    finally
    {
        // stop sessions
        session1.Close();
        session2.Close();

        // drop node
        await _coordinationClient.DropNode(nodePath);
    }
}
     */

    /*
    [Fact]
    public async Task DescribeNode()
    {
        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };

        var pathNode = "/local/test";
        try
        {
            await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        }
        catch (YdbException e)
        {
            Console.WriteLine(e.Message);
        }
    }
    */
}
