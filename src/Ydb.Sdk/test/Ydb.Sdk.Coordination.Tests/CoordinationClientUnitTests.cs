using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Coordination.Description;
using NodeConsistencyMode = Ydb.Sdk.Coordination.Description.ConsistencyMode;
using YdbOperation = Ydb.Operations.Operation;

namespace Ydb.Sdk.Coordination.Tests;

public class CoordinationClientUnitTests
{
    [Fact]
    public async Task CreateNodeAsync_SendsCreateNodeRequest_WithResolvedAbsolutePath()
    {
        var (driver, captured) = SetupUnaryDriver<CreateNodeRequest, CreateNodeResponse>(
            CoordinationService.CreateNodeMethod, _ => EmptyOperation<CreateNodeResponse>());

        var client = new CoordinationClient(driver.Object);

        await client.CreateNodeAsync("foo", new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(2),
            SessionGracePeriod = TimeSpan.FromSeconds(15)
        });

        var request = captured.Single();
        Assert.Equal("/local/foo", request.Path);
        Assert.Equal(2000u, request.Config.SelfCheckPeriodMillis);
        Assert.Equal(15000u, request.Config.SessionGracePeriodMillis);
    }

    [Fact]
    public async Task CreateNodeAsync_AbsolutePathPassedThrough()
    {
        var (driver, captured) = SetupUnaryDriver<CreateNodeRequest, CreateNodeResponse>(
            CoordinationService.CreateNodeMethod, _ => EmptyOperation<CreateNodeResponse>());

        var client = new CoordinationClient(driver.Object);

        await client.CreateNodeAsync("/explicit/abs/path", new NodeConfig());

        Assert.Equal("/explicit/abs/path", captured.Single().Path);
    }

    [Fact]
    public async Task AlterNodeAsync_SendsAlterNodeRequest()
    {
        var (driver, captured) = SetupUnaryDriver<AlterNodeRequest, AlterNodeResponse>(
            CoordinationService.AlterNodeMethod, _ => EmptyOperation<AlterNodeResponse>());

        var client = new CoordinationClient(driver.Object);

        await client.AlterNodeAsync("foo", new NodeConfig { SelfCheckPeriod = TimeSpan.FromSeconds(3) });

        var request = captured.Single();
        Assert.Equal("/local/foo", request.Path);
        Assert.Equal(3000u, request.Config.SelfCheckPeriodMillis);
    }

    [Fact]
    public async Task DropNodeAsync_SendsDropNodeRequest()
    {
        var (driver, captured) = SetupUnaryDriver<DropNodeRequest, DropNodeResponse>(
            CoordinationService.DropNodeMethod, _ => EmptyOperation<DropNodeResponse>());

        var client = new CoordinationClient(driver.Object);

        await client.DropNodeAsync("foo");

        Assert.Equal("/local/foo", captured.Single().Path);
    }

    [Fact]
    public async Task DescribeNodeAsync_ParsesConfigFromOperationResult()
    {
        var configResult = new DescribeNodeResult
        {
            Config = new Config
            {
                SelfCheckPeriodMillis = 1500,
                SessionGracePeriodMillis = 12000,
                ReadConsistencyMode = Ydb.Coordination.ConsistencyMode.Strict
            }
        };

        var (driver, _) = SetupUnaryDriver<DescribeNodeRequest, DescribeNodeResponse>(
            CoordinationService.DescribeNodeMethod, _ => new DescribeNodeResponse
            {
                Operation = new YdbOperation
                {
                    Ready = true,
                    Result = Any.Pack(configResult)
                }
            });

        var client = new CoordinationClient(driver.Object);

        var node = await client.DescribeNodeAsync("foo");

        Assert.Equal(TimeSpan.FromMilliseconds(1500), node.SelfCheckPeriod);
        Assert.Equal(TimeSpan.FromMilliseconds(12000), node.SessionGracePeriod);
        Assert.Equal(NodeConsistencyMode.Strict, node.ReadConsistencyMode);
    }

    [Fact]
    public void CreateNodeAsync_EmptyPath_Throws()
    {
        var driver = new Mock<IDriver>();
        driver.Setup(d => d.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(d => d.Database).Returns("/local");

        var client = new CoordinationClient(driver.Object);

        Assert.Throws<ArgumentException>(() => client.CreateNodeAsync("", new NodeConfig()).GetAwaiter().GetResult());
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static (Mock<IDriver> driver, List<TReq> captured) SetupUnaryDriver<TReq, TResp>(
        Method<TReq, TResp> method, Func<TReq, TResp> reply)
        where TReq : class
        where TResp : class
    {
        var captured = new List<TReq>();
        var driver = new Mock<IDriver>();

        driver.Setup(d => d.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(d => d.Database).Returns("/local");
        driver.Setup(d => d.UnaryCall(method, It.IsAny<TReq>(), It.IsAny<GrpcRequestSettings>()))
            .Returns<Method<TReq, TResp>, TReq, GrpcRequestSettings>((_, req, _) =>
            {
                captured.Add(req);
                return Task.FromResult(reply(req));
            });

        return (driver, captured);
    }

    private static T EmptyOperation<T>() where T : class, new()
    {
        var t = new T();
        var prop = typeof(T).GetProperty("Operation");
        prop?.SetValue(t, new YdbOperation { Ready = true, Status = StatusIds.Types.StatusCode.Success });
        return t;
    }
}
