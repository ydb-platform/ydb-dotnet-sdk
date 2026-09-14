using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.OpenTelemetry;
using Ydb.Sdk.Topic.Writer;

namespace Ydb.Sdk.Topic.Tests;

public class WriterMetricsReporterTests
{
    [Fact]
    public void WrittenMessages_ReportsConfirmedMessages()
    {
        const string metricName = "ydb.topic.writer.written.messages";
        var exportedItems = new List<Metric>();
        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems)
            .Build();
        var reporter = new WriterMetricsReporter("localhost:2136", "/local", "/topic", "writer");

        reporter.ReportWritten(PersistenceStatus.Written);
        reporter.ReportWritten(PersistenceStatus.AlreadyWritten);
        Assert.True(meterProvider.ForceFlush());

        var metric = Assert.Single(exportedItems, metric => metric.Name == metricName);
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("{message}", metric.Unit);
        var statuses = new Dictionary<string, long>();
        foreach (var point in metric.GetMetricPoints())
        {
            var tags = GetTags(point);
            Assert.Equal(5, tags.Count);
            Assert.Equal("localhost:2136", tags["endpoint"]);
            Assert.Equal("/local", tags["database"]);
            Assert.Equal("/topic", tags["topic"]);
            Assert.Equal("writer", tags["writer.name"]);
            statuses.Add(Assert.IsType<string>(tags["status"]), point.GetSumLong());
        }

        Assert.Equal(2, statuses.Count);
        Assert.Equal(1, statuses["written"]);
        Assert.Equal(1, statuses["already_written"]);
    }

    private static Dictionary<string, object?> GetTags(MetricPoint point)
    {
        var tags = new Dictionary<string, object?>();
        foreach (var tag in point.Tags)
        {
            tags.Add(tag.Key, tag.Value);
        }

        return tags;
    }
}
