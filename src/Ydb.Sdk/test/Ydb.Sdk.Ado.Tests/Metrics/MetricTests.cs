// Pattern: https://github.com/npgsql/npgsql/blob/main/test/Npgsql.Tests/MetricTests.cs

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests.Metrics;

[Collection("DisableParallelization")]
public class MetricTests : TestBase
{
    private static readonly YdbConnectionStringBuilder ConnectionSettings = new(TestUtils.ConnectionString);

    [Fact]
    public async Task OperationDuration()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddMeter("Ydb.Sdk")
            .AddInMemoryExporter(exportedItems)
            .Build();

        await using var dataSource = new YdbDataSource(TestUtils.ConnectionString);
        await using var conn = await dataSource.OpenConnectionAsync();
        await using var cmd = new YdbCommand("SELECT 1;", conn);
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
            }
        }

        meterProvider.ForceFlush();

        var metric = exportedItems.SingleOrDefault(m => m.Name == "db.client.operation.duration");
        Assert.NotNull(metric);

        var point = GetFilteredPoints(metric!.GetMetricPoints()).Single();

        Assert.True(point.GetHistogramSum() > 0);
        Assert.Equal(1, point.GetHistogramCount());

        var tags = ToDictionary(point.Tags);
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(ConnectionSettings.Database, tags["db.namespace"]);
        Assert.Equal(ConnectionSettings.Host, tags["server.address"]);
        Assert.Equal(ConnectionSettings.Port.ToString(), tags["server.port"]?.ToString());
    }

    [Fact]
    public async Task ConnectionCount()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddMeter("Ydb.Sdk")
            .AddInMemoryExporter(exportedItems)
            .Build();

        await using var dataSource = new YdbDataSource(TestUtils.ConnectionString);

        await using (var _ = await dataSource.OpenConnectionAsync())
        {
            meterProvider.ForceFlush();

            var metric = exportedItems.Single(m => m.Name == "db.client.connection.count");
            var points = GetFilteredPoints(metric.GetMetricPoints());

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(1, usedPoint.GetSumLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(0, idlePoint.GetSumLong());

            exportedItems.Clear();
        }

        meterProvider.ForceFlush();

        {
            var metric = exportedItems.Single(m => m.Name == "db.client.connection.count");
            var points = GetFilteredPoints(metric.GetMetricPoints());

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(0, usedPoint.GetSumLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(1, idlePoint.GetSumLong());
        }
    }

    [Fact]
    public async Task CommandFailure_IncrementsFailedCounter()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddMeter("Ydb.Sdk")
            .AddInMemoryExporter(exportedItems)
            .Build();

        await using var dataSource = new YdbDataSource(TestUtils.ConnectionString);
        await using var conn = await dataSource.OpenConnectionAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await new YdbCommand("SELECT * FROM table_that_does_not_exist_xyz", conn).ExecuteScalarAsync());

        meterProvider.ForceFlush();

        var failed = exportedItems.SingleOrDefault(m => m.Name == "db.client.operation.failed");
        Assert.NotNull(failed);
        var point = GetFilteredPoints(failed!.GetMetricPoints()).Single();
        Assert.Equal(1, point.GetSumLong());
        var tags = ToDictionary(point.Tags);
        Assert.Equal(ConnectionSettings.Database, tags["db.namespace"]);
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
            if (tags.GetValueOrDefault("db.namespace") as string == ConnectionSettings.Database &&
                tags.GetValueOrDefault("server.address") as string == ConnectionSettings.Host)
            {
                yield return point;
            }
        }
    }
}
