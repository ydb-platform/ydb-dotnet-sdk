using Xunit;
using Ydb.Coordination;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using ConsistencyMode = Ydb.Sdk.Coordination.Description.ConsistencyMode;
using RateLimiterCountersMode = Ydb.Sdk.Coordination.Description.RateLimiterCountersMode;

namespace Ydb.Sdk.Coordination.Tests;

public class CoordinationClientIntegrationTests
{
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);

    //  Given, When, Then
    [Fact]
    public async Task CreateNode()
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
        var describeCoordinationNodeSettings = new DescribeCoordinationNodeSettings();
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        var oldNodeConfig = coordinationNodeSettings.Config.ToProto();
        var pathNode = "/local/test";

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        var describeNode = await _coordinationClient.DescribeNode(pathNode, describeCoordinationNodeSettings);
        var describeNodeConfig = describeNode.ToProto();

        //Then
        Assert.Equal(oldNodeConfig.SelfCheckPeriodMillis, describeNodeConfig.SelfCheckPeriodMillis);
        Assert.Equal(oldNodeConfig.SessionGracePeriodMillis, describeNodeConfig.SessionGracePeriodMillis);
        Assert.Equal(oldNodeConfig.ReadConsistencyMode, describeNodeConfig.ReadConsistencyMode);
        Assert.Equal(oldNodeConfig.AttachConsistencyMode, describeNodeConfig.AttachConsistencyMode);
        Assert.Equal(oldNodeConfig.RateLimiterCountersMode, describeNodeConfig.RateLimiterCountersMode);
        await _coordinationClient.DropNode(pathNode,dropCoordinationNodeSettings);
    }


    [Fact]
    public async Task AlterNode()
    {
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
        var alterNodeConfig = alterCoordinationNodeSettings.Config.ToProto();
        var pathNode = "/local/test";

        // When
        await _coordinationClient.CreateNode(pathNode, coordinationNodeSettings);
        await _coordinationClient.AlterNode(pathNode, alterCoordinationNodeSettings);

        var describeNode = await _coordinationClient.DescribeNode(pathNode, describeCoordinationNodeSettings);
        var describeNodeConfig = describeNode.ToProto();

        
        //Then
        Assert.Equal(alterNodeConfig.SelfCheckPeriodMillis, describeNodeConfig.SelfCheckPeriodMillis);
        Assert.Equal(alterNodeConfig.SessionGracePeriodMillis, describeNodeConfig.SessionGracePeriodMillis);
        Assert.Equal(alterNodeConfig.ReadConsistencyMode, describeNodeConfig.ReadConsistencyMode);
        Assert.Equal(alterNodeConfig.AttachConsistencyMode, describeNodeConfig.AttachConsistencyMode);
        Assert.Equal(alterNodeConfig.RateLimiterCountersMode, describeNodeConfig.RateLimiterCountersMode);
        await _coordinationClient.DropNode(pathNode,dropCoordinationNodeSettings);
    }

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
