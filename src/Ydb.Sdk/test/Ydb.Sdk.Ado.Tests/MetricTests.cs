using System.Diagnostics;
using System.Linq;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

[Collection("DisableParallelization")]
public class MetricTests : TestBase
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

        await using (var reader = await new YdbCommand("SELECT 1;", conn).ExecuteReaderAsync())
            while (await reader.ReadAsync()) { }

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

        var points = GetFilteredPoints(metric!.GetMetricPoints())
            .ToDictionary(p => (string)ToDictionary(p.Tags)["db.operation.name"]!);

        Assert.True(points["ExecuteQuery"].GetHistogramSum() > 0);
        Assert.True(points["CreateSession"].GetHistogramSum() > 0);
        Assert.True(points["Commit"].GetHistogramSum() > 0);
        Assert.True(points["Rollback"].GetHistogramSum() > 0);

        var tags = ToDictionary(points["ExecuteQuery"].Tags);
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(settings.Database, tags["db.namespace"]);
        Assert.Equal(settings.Host, tags["server.address"]);
        Assert.Equal(settings.Port.ToString(), tags["server.port"]?.ToString());
        Assert.Equal("ExecuteQuery", tags["db.operation.name"]);
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

            var metric = GetMetric(exportedItems, "db.client.connection.count");
            var points = GetFilteredPoints(metric.GetMetricPoints());

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(1, usedPoint.GetSumLong());
            Assert.Equal(settings.PoolName, ToDictionary(usedPoint.Tags)["db.client.connection.pool.name"]);

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(0, idlePoint.GetSumLong());

            exportedItems.Clear();
        }

        meterProvider.ForceFlush();

        {
            var metric = GetMetric(exportedItems, "db.client.connection.count");
            var points = GetFilteredPoints(metric.GetMetricPoints());

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(0, usedPoint.GetSumLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(1, idlePoint.GetSumLong());
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

        var failed = GetMetric(exportedItems, "db.client.operation.failed");
        Assert.NotNull(failed);
        var point = GetFilteredPoints(failed!.GetMetricPoints()).Single();
        Assert.Equal(1, point.GetSumLong());

        var tags = ToDictionary(point.Tags);
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(settings.Database, tags["db.namespace"]);
        Assert.Equal("ExecuteQuery", tags["db.operation.name"]);
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

        var metric = GetMetric(exportedItems, "db.client.connection.create_time");
        var point = GetFilteredPoints(metric.GetMetricPoints()).Single();

        Assert.True(point.GetHistogramSum() > 0);
        var tags = ToDictionary(point.Tags);
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(settings.Database, tags["db.namespace"]);
        Assert.Equal(settings.PoolName, tags["db.client.connection.pool.name"]);
    }

    [Fact]
    public async Task ConnectionWaitTime()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.CreateSessionTimeout = 5;
            builder.PoolName = "ado-metrics-wait";
        });

        await using var dataSource = new YdbDataSource(settings);
        await using var firstConn = await dataSource.OpenConnectionAsync();

        var secondConnectionTask = dataSource.OpenConnectionAsync();
        await WaitForPendingRequestsAsync(exportedItems, meterProvider, 1);

        await firstConn.DisposeAsync();
        await using var secondConn = await secondConnectionTask;

        exportedItems.Clear();
        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "db.client.connection.wait_time");
        var point = GetFilteredPoints(metric.GetMetricPoints()).Single();
        Assert.True(point.GetHistogramSum() > 0);
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
        await using var firstConn = await dataSource.OpenConnectionAsync();

        var secondConnectionTask = dataSource.OpenConnectionAsync();
        var pendingPoint = await WaitForPendingRequestsAsync(exportedItems, meterProvider, 1);

        var pendingTags = ToDictionary(pendingPoint.Tags);
        Assert.Equal("ydb", pendingTags["db.system.name"]);
        Assert.Equal(settings.Database, pendingTags["db.namespace"]);
        Assert.Equal(settings.PoolName, pendingTags["db.client.connection.pool.name"]);

        await firstConn.DisposeAsync();
        await using var secondConn = await secondConnectionTask;
        await WaitForPendingRequestsAsync(exportedItems, meterProvider, 0);
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

        var metric = GetMetric(exportedItems, "db.client.connection.timeouts");
        Assert.NotNull(metric);

        var point = GetFilteredPoints(metric!.GetMetricPoints()).Single();
        Assert.Equal(1, point.GetSumLong());
        Assert.Equal(settings.PoolName, ToDictionary(point.Tags)["db.client.connection.pool.name"]);
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

    private static async Task<MetricPoint> WaitForPendingRequestsAsync(
        List<Metric> exportedItems,
        MeterProvider meterProvider,
        long expectedValue)
    {
        for (var i = 0; i < 30; i++)
        {
            exportedItems.Clear();
            meterProvider.ForceFlush();

            var metric = exportedItems.SingleOrDefault(m => m.Name == "db.client.connection.pending_requests");
            if (metric != null)
            {
                foreach (var point in GetFilteredPoints(metric.GetMetricPoints()))
                {
                    if (point.GetGaugeLastValueLong() == expectedValue)
                    {
                        return point;
                    }
                }
            }

            await Task.Delay(100);
        }

        Assert.Fail($"Point with pending requests value '{expectedValue}' not found");
        throw new UnreachableException();
    }

    private static MetricPoint GetPoint(IEnumerable<MetricPoint> points, string state)
    {
        foreach (var point in points)
        {
            foreach (var tag in point.Tags)
            {
                if (tag.Key == "db.client.connection.state" && (string?)tag.Value == state)
                {
                    return point;
                }
            }
        }

        Assert.Fail($"Point with state '{state}' not found");
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
