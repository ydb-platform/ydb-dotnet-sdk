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

        //Then
        await semaphore.Delete(false);
        coordinationSession.Close();
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
        var describeSemaphoreResultBefore = await semaphore2.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);
        //сравниваем значения

        await semaphore1.Update(semaphoreData2);
        var describeSemaphoreResultAfter = await semaphore2.Describe(DescribeSemaphoreMode.WithOwnersAndWaiters);

        //сравниваем значения

        //Then
        await semaphore2.Delete(false);
        coordinationSession1.Close();
        coordinationSession2.Close();
        await _coordinationClient.DropNode(pathNode, dropCoordinationNodeSettings);
    }

    [Fact]
    public async Task AcquireSemaphore()
    {
    }

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
        await lease2.ReleaseAsync();

        var lease1 = await lease1Task; // теперь должен завершиться

        // release second lease
        await lease1.ReleaseAsync();

        // delete semaphore
        await semaphore2.Delete(false);

        // take first ephemeral lease
        lease2Task = semaphore2.Acquire(1, ephemeral: true, data: null, timeout);
        lease2 = await lease2Task;

        // request second ephemeral lease, waiting
        lease1Task = semaphore1.Acquire(1, ephemeral: true, data: null, timeout);

        Assert.False(lease1Task.IsCompleted);

        // release first → второй завершается
        await lease2.ReleaseAsync();

        lease1 = await lease1Task;

        // release second
        await lease1.ReleaseAsync();
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
