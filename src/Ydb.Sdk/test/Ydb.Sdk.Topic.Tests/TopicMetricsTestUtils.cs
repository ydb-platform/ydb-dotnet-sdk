using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.OpenTelemetry;

namespace Ydb.Sdk.Topic.Tests;

internal static class TopicMetricsTestUtils
{
    internal static MeterProvider CreateMeterProvider(List<Metric> exportedItems, params string[] disabledMetrics)
    {
        var builder = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems);
        foreach (var name in disabledMetrics)
        {
            builder.AddView(name, MetricStreamConfiguration.Drop);
        }

        return builder.Build();
    }

    internal static Metric GetMetric(List<Metric> exportedItems, string name) =>
        Assert.Single(exportedItems, metric => metric.Name == name);

    internal static IEnumerable<MetricPoint> GetPoints(
        List<Metric> exportedItems, string name, string tagName, string tagValue) =>
        exportedItems.Where(metric => metric.Name == name)
            .SelectMany(EnumeratePoints)
            .Where(point => Equals(ToDictionary(point.Tags).GetValueOrDefault(tagName), tagValue));

    internal static IEnumerable<MetricPoint> EnumeratePoints(Metric metric)
    {
        foreach (var point in metric.GetMetricPoints())
        {
            yield return point;
        }
    }

    internal static Dictionary<string, object?> GetTags(MetricPoint point) => ToDictionary(point.Tags);

    internal static Dictionary<string, object?> ToDictionary(ReadOnlyTagCollection tags)
    {
        var tagsByName = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            tagsByName.Add(tag.Key, tag.Value);
        }

        return tagsByName;
    }
}
