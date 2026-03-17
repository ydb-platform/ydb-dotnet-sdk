using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using System.Diagnostics.Metrics;


namespace Ydb.Sdk.Ado.Tests.Tracing;

[Collection("DisableParallelization")]

public class YdbMetricsTests : TestBase
{
    private static readonly YdbConnectionStringBuilder ConnectionSettings = new(TestUtils.ConnectionString);
    private const string MeterName = "Ydb.Sdk";
    
    [Fact]
    public async Task CommandExecute_EmitsDurationAndSuccessMetrics()
    {
        using var listener = new MeterListener();
        var measurements = new List<CapturedMeasurement>();

        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == MeterName)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };

        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
        {
            var tagsDict = tags.ToArray().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            measurements.Add(new CapturedMeasurement(instrument.Name, measurement, tagsDict));
        });
        listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
        {
            var tagsDict = tags.ToArray().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            measurements.Add(new CapturedMeasurement(instrument.Name, measurement, tagsDict));
        });

        listener.Start();

        await using var connection = await CreateOpenConnectionAsync();
        _ = await new YdbCommand("SELECT 42;", connection).ExecuteScalarAsync();

        var duration = measurements.FirstOrDefault(m => m.Name == "db.client.operation.duration");
        Assert.NotNull(duration);
        Assert.True((double)duration.Value > 0);
        AssertCommonMetricTags(duration.Tags);

        var failed = measurements.Any(m => m.Name == "db.client.operation.failed");
        Assert.False(failed, "Failed counter should not be incremented on success");
    }
    
    [Fact]
    public async Task CommandExecute_Error_IncrementsFailedCounter()
    {
        using var listener = new MeterListener();
        var measurements = new List<CapturedMeasurement>();

        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == MeterName)
                meterListener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
        {
            var tagsDict = tags.ToArray().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            measurements.Add(new CapturedMeasurement(instrument.Name, measurement, tagsDict));
        });
        listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
        {
            var tagsDict = tags.ToArray().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            measurements.Add(new CapturedMeasurement(instrument.Name, measurement, tagsDict));
        });

        listener.Start();

        await using var connection = await CreateOpenConnectionAsync();
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await new YdbCommand("SELECT * FROM non_existing_table", connection).ExecuteScalarAsync());

        var failedMetric = measurements.FirstOrDefault(m => m.Name == "db.client.operation.failed");
        Assert.NotNull(failedMetric);
        Assert.Equal(1, (int)failedMetric.Value);
        AssertCommonMetricTags(failedMetric.Tags);

        var durationMetric = measurements.FirstOrDefault(m => m.Name == "db.client.operation.duration");
        Assert.NotNull(durationMetric);
    }
    
    [Fact]
    public async Task CommandExecute_UpdatesExecutingCounter()
    {
        using var listener = new MeterListener();
        var measurements = new List<CapturedMeasurement>();

        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == MeterName && instrument.Name == "db.client.operation.ydb.executing")
                meterListener.EnableMeasurementEvents(instrument);
        };

        listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
        {
            var tagsDict = tags.ToArray().ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            measurements.Add(new CapturedMeasurement(instrument.Name, measurement, tagsDict));
        });

        listener.Start();

        await using var connection = await CreateOpenConnectionAsync();
        _ = await new YdbCommand("SELECT 1;", connection).ExecuteScalarAsync();

        var executingMeasurements = measurements
            .Where(m => m.Name == "db.client.operation.ydb.executing")
            .Select(m => (int)m.Value)
            .ToList();

        Assert.Contains(1, executingMeasurements);
        Assert.Contains(-1, executingMeasurements);
    }
    
    private static void AssertCommonMetricTags(IReadOnlyDictionary<string, object?> tags)
    {
        Assert.Equal("ydb", tags["db.system.name"]);
        Assert.Equal(ConnectionSettings.Database, tags["db.namespace"]);
        Assert.Equal(ConnectionSettings.Host, tags["server.address"]);
        Assert.Equal(ConnectionSettings.Port.ToString(), tags["server.port"]?.ToString());
    }

    private record CapturedMeasurement(
        string Name, 
        object Value, 
        IReadOnlyDictionary<string, object?> Tags
    );
}
