using System.Diagnostics.Metrics;
using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

[Collection("DisableParallelization")]
public class YdbExecutionStrategyMetricTests
{
    private const string FakeConnectionString = "Host=localhost;Port=2136;Database=/test";
    private const string RetryDurationMetric = "ydb.client.retry.duration";
    private const string RetryAttemptsMetric = "ydb.client.retry.attempts";

    [Fact]
    public async Task RetryMetrics_FirstTrySuccess_RecordsOneAttempt()
    {
        var operationName = NewOperationName();
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            OperationName = operationName
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync<int, int>(
            state: 0,
            operation: (_, _, _) => Task.FromResult(0),
            verifySucceeded: null);

        var attempts = SingleAttemptForOperation(measurements, operationName);
        Assert.Equal(1, attempts);

        var duration = SingleDurationForOperation(measurements, operationName);
        Assert.True(duration >= 0);
    }

    [Fact]
    public async Task RetryMetrics_WithRetries_RecordsTotalAttempts()
    {
        var operationName = NewOperationName();
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 5,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1,
            OperationName = operationName
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();

        var calls = 0;
        await strategy.ExecuteAsync<int, int>(
            state: 0,
            operation: (_, _, _) =>
            {
                calls++;
                if (calls < 3)
                    throw new YdbException(StatusCode.Aborted, "retry me");
                return Task.FromResult(0);
            },
            verifySucceeded: null);

        Assert.Equal(3, calls);
        Assert.Equal(3, SingleAttemptForOperation(measurements, operationName));
    }

    [Fact]
    public async Task RetryMetrics_NonRetryableError_StillRecordsOneAttempt()
    {
        var operationName = NewOperationName();
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            OperationName = operationName
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await Assert.ThrowsAsync<YdbException>(() =>
            strategy.ExecuteAsync<int, int>(
                state: 0,
                operation: (_, _, _) => throw new YdbException(StatusCode.Unauthorized, "no"),
                verifySucceeded: null));

        Assert.Equal(1, SingleAttemptForOperation(measurements, operationName));
        Assert.True(SingleDurationForOperation(measurements, operationName) >= 0);
    }

    [Fact]
    public async Task RetryMetrics_RetriesExhausted_RecordsAllAttempts()
    {
        var operationName = NewOperationName();
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 3,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1,
            OperationName = operationName
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await Assert.ThrowsAsync<RetryLimitExceededException>(() =>
            strategy.ExecuteAsync<int, int>(
                state: 0,
                operation: (_, _, _) => throw new YdbException(StatusCode.Aborted, "always fails"),
                verifySucceeded: null));

        // MaxAttempts=3 -> initial + 2 retries -> 3 total attempts.
        Assert.Equal(3, SingleAttemptForOperation(measurements, operationName));
    }

    [Fact]
    public async Task RetryMetrics_NoOperationName_OmitsOperationNameTag()
    {
        await using var db = CreateContext();
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync<int, int>(
            state: 0,
            operation: (_, _, _) => Task.FromResult(0),
            verifySucceeded: null);

        // Both metrics must contain at least one record without an operation.name tag.
        Assert.Contains(measurements,
            m => m.InstrumentName == RetryAttemptsMetric && !m.Tags.ContainsKey("operation.name"));
        Assert.Contains(measurements,
            m => m.InstrumentName == RetryDurationMetric && !m.Tags.ContainsKey("operation.name"));
    }

    [Fact]
    public async Task RetryMetrics_WithOperationName_TagsBothMetrics()
    {
        var operationName = NewOperationName();
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            OperationName = operationName
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync<int, int>(
            state: 0,
            operation: (_, _, _) => Task.FromResult(0),
            verifySucceeded: null);

        var attempts = measurements.Single(m =>
            m.InstrumentName == RetryAttemptsMetric &&
            (string?)m.Tags.GetValueOrDefault("operation.name") == operationName);
        Assert.Equal(operationName, attempts.Tags["operation.name"]);

        var duration = measurements.Single(m =>
            m.InstrumentName == RetryDurationMetric &&
            (string?)m.Tags.GetValueOrDefault("operation.name") == operationName);
        Assert.Equal(operationName, duration.Tags["operation.name"]);
    }

    private static TestDbContext CreateContext(Action<YdbDbContextOptionsBuilder>? configure = null) =>
        new(configure);

    private sealed class TestDbContext(Action<YdbDbContextOptionsBuilder>? configure) : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseYdb(FakeConnectionString, ydb => configure?.Invoke(ydb));
    }

    private sealed record Measurement(
        string InstrumentName,
        double Value,
        IReadOnlyDictionary<string, object?> Tags);

    private static MeterListener StartMeterListener(out List<Measurement> measurements)
    {
        var captured = new List<Measurement>();
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, ml) =>
            {
                if (instrument.Meter.Name != "Ydb.Sdk") return;
                if (instrument.Name is RetryDurationMetric or RetryAttemptsMetric)
                    ml.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            captured.Add(new Measurement(instrument.Name, value, ToDictionary(tags))));
        listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) =>
            captured.Add(new Measurement(instrument.Name, value, ToDictionary(tags))));
        listener.Start();

        measurements = captured;
        return listener;
    }

    private static double SingleAttemptForOperation(List<Measurement> measurements, string operationName) =>
        measurements.Single(m =>
                m.InstrumentName == RetryAttemptsMetric &&
                (string?)m.Tags.GetValueOrDefault("operation.name") == operationName)
            .Value;

    private static double SingleDurationForOperation(List<Measurement> measurements, string operationName) =>
        measurements.Single(m =>
                m.InstrumentName == RetryDurationMetric &&
                (string?)m.Tags.GetValueOrDefault("operation.name") == operationName)
            .Value;

    private static Dictionary<string, object?> ToDictionary(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var tag in tags)
            dict[tag.Key] = tag.Value;
        return dict;
    }

    private static string NewOperationName() => "EFCore.Test." + Guid.NewGuid().ToString("N");
}
