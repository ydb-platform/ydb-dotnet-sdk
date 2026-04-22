using System.Diagnostics;
using Grpc.Core;
using Moq;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

[Collection("DisableParallelization")]
public class YdbMetricTests : TestBase
{
    private static readonly YdbConnectionStringBuilder BaseConnectionSettings = new(TestUtils.ConnectionString)
    {
        PoolName = "ado-metrics-tests"
    };

    [Fact]
    public async Task OperationDuration()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);
        await using var conn = await dataSource.OpenConnectionAsync();

        await new YdbCommand("SELECT 1;", conn).ExecuteNonQueryAsync();

        await using var txConn = await dataSource.OpenConnectionAsync();
        await using var tx = await txConn.BeginTransactionAsync();
        await new YdbCommand("SELECT 1;", txConn).ExecuteNonQueryAsync();
        await tx.CommitAsync();

        await using var rollbackConn = await dataSource.OpenConnectionAsync();
        await using var rollbackTx = await rollbackConn.BeginTransactionAsync();
        await rollbackTx.RollbackAsync();

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "db.client.operation.duration");
        Assert.NotNull(metric);

        var points = GetFilteredPoints(metric.GetMetricPoints())
            .ToDictionary(p => (string)ToDictionary(p.Tags)["ydb.operation.name"]!);

        Assert.True(points["ExecuteQuery"].GetHistogramSum() > 0);
        Assert.True(points["Commit"].GetHistogramSum() > 0);
        Assert.True(points["Rollback"].GetHistogramSum() > 0);

        var tags = ToDictionary(points["ExecuteQuery"].Tags);
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(settings.Database, tags["db.namespace"]);
        Assert.Equal(settings.Host, tags["server.address"]);
        Assert.Equal(settings.Port.ToString(), tags["server.port"]?.ToString());
        Assert.Equal("ExecuteQuery", tags["ydb.operation.name"]);
    }

    [Fact]
    public async Task ConnectionCount()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);

        await using (var _ = await dataSource.OpenConnectionAsync())
        {
            meterProvider.ForceFlush();

            var metric = GetMetric(exportedItems, "ydb.query.session.count");
            var points = GetConnectionCountPoints(metric.GetMetricPoints(), settings.PoolName!).ToList();

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(1, usedPoint.GetGaugeLastValueLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(0, idlePoint.GetGaugeLastValueLong());

            exportedItems.Clear();
        }

        meterProvider.ForceFlush();

        {
            var metric = GetMetric(exportedItems, "ydb.query.session.count");
            var points = GetConnectionCountPoints(metric.GetMetricPoints(), settings.PoolName!).ToList();

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(0, usedPoint.GetGaugeLastValueLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(1, idlePoint.GetGaugeLastValueLong());
        }
    }

    [Fact]
    public async Task OperationFailed()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);
        await using var conn = await dataSource.OpenConnectionAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await new YdbCommand("SELECT * FROM table_that_does_not_exist_xyz", conn).ExecuteScalarAsync());

        meterProvider.ForceFlush();

        var failed = GetMetric(exportedItems, "ydb.client.operation.failed");
        Assert.NotNull(failed);
        var point = GetFilteredPoints(failed.GetMetricPoints()).Single();
        Assert.Equal(1, point.GetSumLong());

        var tags = ToDictionary(point.Tags);
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(settings.Database, tags["db.namespace"]);
        Assert.Equal("ExecuteQuery", tags["ydb.operation.name"]);
    }

    [Fact]
    public async Task OperationFailed_CreateSessionUnaryCall()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder => builder.PoolName = "ado-metrics-create-session-unary");
        var driver = CreateMockDriver();
        driver.Setup(d => d.UnaryCall(
                QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(s => s.ClientCapabilities.Contains("session-balancer"))))
            .ThrowsAsync(new YdbException(
                new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.ResourceExhausted, "Mock exhausted"))));

        var factory = new PoolingSessionFactory(driver.Object, settings);
        await using var source = new PoolingSessionSource<PoolingSession>(factory, settings);
        var session = factory.NewSession(source);

        var ex = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal(StatusCode.ClientTransportResourceExhausted, ex.Code);

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.client.operation.failed");
        var point = GetOperationFailedPoint(
            metric.GetMetricPoints(),
            settings,
            operationName: "CreateSession",
            statusCode: "ClientTransportResourceExhausted");

        Assert.Equal(1, point.GetSumLong());
    }

    [Fact]
    public async Task OperationFailed_CreateSessionAttachStream()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder => builder.PoolName = "ado-metrics-create-session-attach");
        var driver = CreateMockDriver();
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);

        driver.Setup(d => d.UnaryCall(
                QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(s => s.ClientCapabilities.Contains("session-balancer"))))
            .ReturnsAsync(new CreateSessionResponse
            {
                Status = StatusIds.Types.StatusCode.Success,
                SessionId = "sessionId",
                NodeId = 3
            });

        driver.Setup(d => d.ServerStreamCall(
                QueryService.AttachSessionMethod,
                It.Is<AttachSessionRequest>(r => r.SessionId == "sessionId"),
                It.Is<GrpcRequestSettings>(s => s.NodeId == 3)))
            .ReturnsAsync(attachStream.Object);

        attachStream.Setup(s => s.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new YdbException(
                new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.ResourceExhausted, "Mock exhausted"))));
        attachStream.Setup(s => s.Dispose());

        var factory = new PoolingSessionFactory(driver.Object, settings);
        await using var source = new PoolingSessionSource<PoolingSession>(factory, settings);
        var session = factory.NewSession(source);

        var ex = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal(StatusCode.ClientTransportResourceExhausted, ex.Code);

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.client.operation.failed");
        var point = GetOperationFailedPoint(
            metric.GetMetricPoints(),
            settings,
            operationName: "CreateSession",
            statusCode: "ClientTransportResourceExhausted");

        Assert.Equal(1, point.GetSumLong());
    }

    [Fact]
    public async Task ConnectionCreateTime()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);
        await using var _ = await dataSource.OpenConnectionAsync();

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.query.session.create_time");
        var point = GetPoolPoints(metric.GetMetricPoints(), settings.PoolName!).Single();

        Assert.True(point.GetHistogramSum() > 0);
        Assert.Equal(settings.PoolName, ToDictionary(point.Tags)["ydb.query.session.pool.name"]);
    }

    [Fact]
    public async Task ConnectionPendingRequests()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.CreateSessionTimeout = 5;
            builder.PoolName = "ado-metrics-pending";
        });

        await using var dataSource = new YdbDataSource(settings);
        var firstConn = await dataSource.OpenConnectionAsync();

        var secondConnectionTask = dataSource.OpenConnectionAsync();
        await Task.Yield(); // let secondConnectionTask reach ReportPendingConnectionRequestStart
        meterProvider.ForceFlush();

        var pendingMetric = GetMetric(exportedItems, "ydb.query.session.pending_requests");
        var pendingPoint = GetPoolPoints(pendingMetric.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(1, pendingPoint.GetSumLong());
        Assert.Equal(settings.PoolName, ToDictionary(pendingPoint.Tags)["ydb.query.session.pool.name"]);

        await firstConn.DisposeAsync();
        await using var secondConn = await secondConnectionTask;

        exportedItems.Clear();
        meterProvider.ForceFlush();
        pendingMetric = GetMetric(exportedItems, "ydb.query.session.pending_requests");
        Assert.Equal(0, GetPoolPoints(pendingMetric.GetMetricPoints(), settings.PoolName!).Single().GetSumLong());
    }

    [Fact]
    public async Task PoolSizeMaxMin()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MinPoolSize = 2;
            builder.MaxPoolSize = 7;
            builder.PoolName = "ado-metrics-max-min";
        });

        await using var dataSource = new YdbDataSource(settings);
        await using var _ = await dataSource.OpenConnectionAsync();

        meterProvider.ForceFlush();

        var max = GetMetric(exportedItems, "ydb.query.session.max");
        var maxPoint = GetPoolPoints(max.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(7, maxPoint.GetGaugeLastValueLong());
        Assert.Equal(settings.PoolName, ToDictionary(maxPoint.Tags)["ydb.query.session.pool.name"]);

        var min = GetMetric(exportedItems, "ydb.query.session.min");
        var minPoint = GetPoolPoints(min.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(2, minPoint.GetGaugeLastValueLong());
        Assert.Equal(settings.PoolName, ToDictionary(minPoint.Tags)["ydb.query.session.pool.name"]);
    }

    [Fact]
    public async Task ConnectionTimeouts()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.CreateSessionTimeout = 1;
            builder.PoolName = "ado-metrics-timeouts";
        });

        await using var dataSource = new YdbDataSource(settings);

        await using var conn = await dataSource.OpenConnectionAsync();

        await Assert.ThrowsAsync<YdbException>(async () => await dataSource.OpenConnectionAsync());

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.query.session.timeouts");
        Assert.NotNull(metric);

        var point = GetPoolPoints(metric.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(1, point.GetSumLong());
        Assert.Equal(settings.PoolName, ToDictionary(point.Tags)["ydb.query.session.pool.name"]);
    }

    [Fact]
    public async Task ImplicitSessionSource_DoesNotPublishPoolMetrics()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder => { builder.EnableImplicitSession = true; });

        await using var dataSource = new YdbDataSource(settings);
        await using var _ = await dataSource.OpenConnectionAsync();

        meterProvider.ForceFlush();

        AssertNoPoolMetricsForPool(exportedItems, settings.PoolName!);
    }

    private static readonly string[] PoolScopedMetricNames =
    [
        "ydb.query.session.count",
        "ydb.query.session.max",
        "ydb.query.session.min",
        "ydb.query.session.timeouts",
        "ydb.query.session.pending_requests",
        "ydb.query.session.create_time"
    ];

    private static void AssertNoPoolMetricsForPool(List<Metric> exportedItems, string poolName)
    {
        foreach (var metric in exportedItems.Where(m => PoolScopedMetricNames.Contains(m.Name)))
        {
            foreach (var point in metric.GetMetricPoints())
            {
                if (ToDictionary(point.Tags).GetValueOrDefault("ydb.query.session.pool.name") as string == poolName)
                {
                    Assert.Fail(
                        $"Implicit session must not publish pool metric '{metric.Name}' for pool '{poolName}'.");
                }
            }
        }
    }

    private static MeterProvider CreateMeterProvider(List<Metric> exportedItems) =>
        OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddMeter("Ydb.Sdk")
            .AddInMemoryExporter(exportedItems)
            .Build();

    private static YdbConnectionStringBuilder CreateConnectionSettings(
        Action<YdbConnectionStringBuilder>? configure = null)
    {
        var settings = new YdbConnectionStringBuilder(TestUtils.ConnectionString)
        {
            PoolName = BaseConnectionSettings.PoolName
        };
        configure?.Invoke(settings);
        return settings;
    }

    private static Metric GetMetric(List<Metric> exportedItems, string name) =>
        exportedItems.Single(m => m.Name == name);

    private static Mock<IDriver> CreateMockDriver()
    {
        var driver = new Mock<IDriver>(MockBehavior.Strict);
        driver.SetupGet(d => d.LoggerFactory).Returns(TestUtils.LoggerFactory);
        driver.Setup(d => d.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return driver;
    }

    private static IEnumerable<MetricPoint> GetConnectionCountPoints(MetricPointsAccessor points, string poolName)
    {
        foreach (var point in points)
        {
            if (ToDictionary(point.Tags).GetValueOrDefault("ydb.query.session.pool.name") as string == poolName)
            {
                yield return point;
            }
        }
    }

    private static MetricPoint GetPoint(IEnumerable<MetricPoint> points, string state)
    {
        foreach (var point in points)
        {
            foreach (var tag in point.Tags)
            {
                if (tag.Key == "ydb.query.session.state" && (string?)tag.Value == state)
                {
                    return point;
                }
            }
        }

        Assert.Fail($"Point with state '{state}' not found");
        throw new UnreachableException();
    }

    private static MetricPoint GetOperationFailedPoint(
        MetricPointsAccessor points,
        YdbConnectionStringBuilder settings,
        string operationName,
        string statusCode)
    {
        foreach (var point in points)
        {
            var tags = ToDictionary(point.Tags);
            if (tags.GetValueOrDefault("db.namespace") as string == settings.Database &&
                tags.GetValueOrDefault("server.address") as string == settings.Host &&
                tags.GetValueOrDefault("ydb.operation.name") as string == operationName &&
                tags.GetValueOrDefault("db.response.status_code") as string == statusCode)
            {
                return point;
            }
        }

        Assert.Fail($"Point for operation '{operationName}' with status '{statusCode}' not found");
        throw new UnreachableException();
    }

    private static Dictionary<string, object?> ToDictionary(ReadOnlyTagCollection tags)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            dict[tag.Key] = tag.Value;
        }

        return dict;
    }

    private static IEnumerable<MetricPoint> GetPoolPoints(MetricPointsAccessor points, string poolName)
    {
        foreach (var point in points)
        {
            if (ToDictionary(point.Tags).GetValueOrDefault("ydb.query.session.pool.name") as string == poolName)
                yield return point;
        }
    }

    private static IEnumerable<MetricPoint> GetFilteredPoints(MetricPointsAccessor points)
    {
        foreach (var point in points)
        {
            var tags = ToDictionary(point.Tags);
            if (tags.GetValueOrDefault("db.namespace") as string == BaseConnectionSettings.Database &&
                tags.GetValueOrDefault("server.address") as string == BaseConnectionSettings.Host)
            {
                yield return point;
            }
        }
    }
}
