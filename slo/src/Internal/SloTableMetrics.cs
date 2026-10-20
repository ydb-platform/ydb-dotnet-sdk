using System.Diagnostics;
using System.Diagnostics.Metrics;
using HdrHistogram;

namespace Internal;

public sealed class SloTableMetrics : IDisposable
{
    private readonly Meter _meter = new("YDB.SLO");
    private readonly Counter<long> _operationsTotal;
    private readonly string _refLabel;
    private readonly Dictionary<string, LatencyAggregator> _latencies = new()
    {
        ["read"] = new(),
        ["write"] = new()
    };
    private readonly Dictionary<string, (double P50, double P95, double P99)> _snapshots = new()
    {
        ["read"] = default,
        ["write"] = default
    };
    private readonly object _snapshotLock = new();
    private readonly Timer _snapshotTimer;

    public SloTableMetrics(string refLabel, int reportPeriod)
    {
        _refLabel = refLabel;
        _operationsTotal = _meter.CreateCounter<long>(
            "sdk.operations.total",
            description: "Total number of operations, by type and status.");

        _meter.CreateObservableGauge(
            "sdk.operation.latency.p50.seconds",
            () => SnapshotMeasurements(s => s.P50),
            unit: "s",
            description: "P50 latency of operations, recomputed each push period.");
        _meter.CreateObservableGauge(
            "sdk.operation.latency.p95.seconds",
            () => SnapshotMeasurements(s => s.P95),
            unit: "s",
            description: "P95 latency of operations, recomputed each push period.");
        _meter.CreateObservableGauge(
            "sdk.operation.latency.p99.seconds",
            () => SnapshotMeasurements(s => s.P99),
            unit: "s",
            description: "P99 latency of operations, recomputed each push period.");

        _snapshotTimer = new Timer(_ =>
        {
            foreach (var (operation, latency) in _latencies)
            {
                var snapshot = latency.SnapshotAndReset();
                lock (_snapshotLock)
                {
                    _snapshots[operation] = snapshot;
                }
            }
        }, null, reportPeriod, reportPeriod);
    }

    public void RecordSuccess(string operation, double seconds)
    {
        var tags = Tags(operation, "success");
        _operationsTotal.Add(1, tags);
        _latencies[operation].Record(seconds);
    }

    public void RecordError(string operation)
    {
        var tags = Tags(operation, "error");
        _operationsTotal.Add(1, tags);
    }

    private TagList Tags(string operation, string status) => new()
    {
        { "operation_type", operation },
        { "operation_status", status },
        { "ref", _refLabel }
    };

    private Measurement<double>[] SnapshotMeasurements(
        Func<(double P50, double P95, double P99), double> selector)
    {
        lock (_snapshotLock)
        {
            return _snapshots.Select(kv => new Measurement<double>(
                selector(kv.Value),
                new KeyValuePair<string, object?>("operation_type", kv.Key),
                new KeyValuePair<string, object?>("operation_status", "success"),
                new KeyValuePair<string, object?>("ref", _refLabel))).ToArray();
        }
    }

    public void Dispose()
    {
        _snapshotTimer.Dispose();
        _meter.Dispose();
    }

    private sealed class LatencyAggregator
    {
        private const long HighestTrackableMicros = 60L * 60L * 1_000_000L;
        private readonly LongHistogram _histogram = new(HighestTrackableMicros, 3);
        private readonly object _lock = new();

        public void Record(double seconds)
        {
            var micros = Math.Clamp((long)(seconds * 1_000_000d), 1, HighestTrackableMicros);
            lock (_lock)
            {
                _histogram.RecordValue(micros);
            }
        }

        public (double P50, double P95, double P99) SnapshotAndReset()
        {
            lock (_lock)
            {
                if (_histogram.TotalCount == 0)
                {
                    return default;
                }

                var snapshot = (
                    _histogram.GetValueAtPercentile(50) / 1_000_000d,
                    _histogram.GetValueAtPercentile(95) / 1_000_000d,
                    _histogram.GetValueAtPercentile(99) / 1_000_000d);
                _histogram.Reset();
                return snapshot;
            }
        }
    }
}
