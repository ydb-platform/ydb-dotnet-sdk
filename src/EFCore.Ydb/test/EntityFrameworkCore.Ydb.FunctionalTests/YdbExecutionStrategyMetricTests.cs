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
        await using var db = CreateContext();
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(
            state: 0,
            operation: (_, _, _) => Task.FromResult(0),
            verifySucceeded: null);

        Assert.Equal(1, SingleAttempts(measurements));
        Assert.True(SingleDuration(measurements) >= 0);
    }

    [Fact]
    public async Task RetryMetrics_WithRetries_RecordsTotalAttempts()
    {
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 5,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();

        var calls = 0;
        await strategy.ExecuteAsync(
            state: 0,
            operation: (_, _, _) =>
            {
                calls++;
                return calls < 3 ? throw new YdbException(StatusCode.Aborted, "retry me") : Task.FromResult(0);
            },
            verifySucceeded: null);

        Assert.Equal(3, calls);
        Assert.Equal(3, SingleAttempts(measurements));
    }

    [Fact]
    public async Task RetryMetrics_NonRetryableError_StillRecordsOneAttempt()
    {
        await using var db = CreateContext();
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await Assert.ThrowsAsync<YdbException>(() =>
            strategy.ExecuteAsync(
                state: 0,
                operation: (_, _, _) => Task.FromException<int>(
                    new YdbException(StatusCode.Unauthorized, "no")),
                verifySucceeded: null));

        // Non-retryable YdbException: original exception rethrown -> total attempts == 1.
        Assert.Equal(1, SingleAttempts(measurements));
        Assert.True(SingleDuration(measurements) >= 0);
    }

    [Fact]
    public async Task RetryMetrics_RetriesExhausted_RecordsAllAttempts()
    {
        await using var db = CreateContext(b => b.UseRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 3,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1
        }));
        using var meterListener = StartMeterListener(out var measurements);

        var strategy = db.Database.CreateExecutionStrategy();
        await Assert.ThrowsAsync<RetryLimitExceededException>(() =>
            strategy.ExecuteAsync(
                state: 0,
                operation: (_, _, _) => Task.FromException<int>(
                    new YdbException(StatusCode.Aborted, "always fails")),
                verifySucceeded: null));

        // MaxAttempts=3 -> 3 actual attempts (initial + 2 retries).
        Assert.Equal(3, SingleAttempts(measurements));
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
        double Value);

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
        listener.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
            captured.Add(new Measurement(instrument.Name, value)));
        listener.SetMeasurementEventCallback<int>((instrument, value, _, _) =>
            captured.Add(new Measurement(instrument.Name, value)));
        listener.Start();

        measurements = captured;
        return listener;
    }

    private static double SingleAttempts(List<Measurement> measurements) =>
        measurements.Single(m => m.InstrumentName == RetryAttemptsMetric).Value;

    private static double SingleDuration(List<Measurement> measurements) =>
        measurements.Single(m => m.InstrumentName == RetryDurationMetric).Value;
}
