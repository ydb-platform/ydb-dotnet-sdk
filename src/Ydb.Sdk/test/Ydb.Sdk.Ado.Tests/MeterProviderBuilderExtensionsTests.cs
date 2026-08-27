using System.Diagnostics.Metrics;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.OpenTelemetry;

namespace Ydb.Sdk.Ado.Tests;

public class MeterProviderBuilderExtensionsTests
{
    [Fact]
    public void AddYdbAdo_RegistersOnlyAdoMeter() =>
        AssertRegistration(builder => builder.AddYdbAdo(), adoRegistered: true, topicRegistered: false);

    [Fact]
    public void AddYdbTopic_RegistersOnlyTopicMeter() =>
        AssertRegistration(builder => builder.AddYdbTopic(), adoRegistered: false, topicRegistered: true);

    [Fact]
    public void AddYdb_RegistersAdoAndTopicMeters() =>
        AssertRegistration(builder => builder.AddYdb(), adoRegistered: true, topicRegistered: true);

    private static void AssertRegistration(
        Func<MeterProviderBuilder, MeterProviderBuilder> register,
        bool adoRegistered,
        bool topicRegistered)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var adoMetricName = $"test.ydb.ado.{suffix}";
        var topicMetricName = $"test.ydb.topic.{suffix}";
        var exportedItems = new List<Metric>();

        using var adoMeter = new Meter("Ydb.Sdk.Ado");
        using var topicMeter = new Meter("Ydb.Sdk.Topic");
        var adoCounter = adoMeter.CreateCounter<int>(adoMetricName);
        var topicCounter = topicMeter.CreateCounter<int>(topicMetricName);
        using var meterProvider = register(global::OpenTelemetry.Sdk.CreateMeterProviderBuilder())
            .AddInMemoryExporter(exportedItems)
            .Build();

        adoCounter.Add(1);
        topicCounter.Add(1);
        meterProvider.ForceFlush();

        Assert.Equal(adoRegistered, exportedItems.Any(metric => metric.Name == adoMetricName));
        Assert.Equal(topicRegistered, exportedItems.Any(metric => metric.Name == topicMetricName));
    }
}
