using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using Ydb.Sdk.Topic;
using Ydb.Sdk.Topic.Writer;

namespace Ydb.Sdk.Coordination.Tests;

public class CoordinationClientIntegrationTests
{
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);

    [Fact]
    public async Task CreateNode()
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
    
    [Fact]
    public async Task AlterNode()
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
}
