// Semantics: https://opentelemetry.io/docs/specs/semconv/database/database-metrics/

using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado;

/// <summary>
/// ADO.NET command and session-pool metrics (OpenTelemetry semantic conventions). One instance per session pool.
/// </summary>
internal sealed class YdbMetricsReporter : IDisposable
{
    private const string Version = "0.1.0";

    private static readonly Histogram<double> CommandDuration;
    private static readonly Counter<int> CommandsFailed;
    private static readonly UpDownCounter<int> CommandsExecuting;

    private static readonly InstrumentAdvice<double> ShortHistogramAdvice = new()
    {
        HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
    };

    private static readonly List<YdbMetricsReporter> Reporters = [];

    private readonly TagList _tags;
    private readonly KeyValuePair<string, object?>[] _tagsForMeasurement;
    private readonly string _sortKey;
    private readonly ISessionSource _sessionPool;

    static YdbMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk", Version);

        CommandDuration = meter.CreateHistogram(
            "db.client.operation.duration",
            unit: "s",
            description: "Duration of database client operations.",
            advice: ShortHistogramAdvice);

        meter.CreateObservableUpDownCounter(
            "db.client.connection.count",
            GetSessionCount,
            unit: "{connection}",
            description: "The number of connections that are currently in state described by the state attribute.");

        CommandsFailed = meter.CreateCounter<int>(
            "db.client.operation.failed",
            unit: "{command}",
            description: "The number of database commands which have failed.");

        CommandsExecuting = meter.CreateUpDownCounter<int>(
            "db.client.operation.ydb.executing",
            unit: "{command}",
            description: "The number of currently executing YDB commands.");
    }

    internal YdbMetricsReporter(ISessionSource sessionPool, YdbConnectionStringBuilder settings)
    {
        _sessionPool = sessionPool;

        _tags = new TagList
        {
            { "db.system.name", "ydb" },
            { "db.namespace", settings.Database },
            { "server.address", settings.Host },
            { "server.port", settings.Port }
        };

        _tagsForMeasurement =
        [
            new KeyValuePair<string, object?>("db.system.name", "ydb"),
            new KeyValuePair<string, object?>("db.namespace", settings.Database),
            new KeyValuePair<string, object?>("server.address", settings.Host),
            new KeyValuePair<string, object?>("server.port", settings.Port)
        ];

        _sortKey = settings.ConnectionString;

        lock (Reporters)
        {
            Reporters.Add(this);
            Reporters.Sort(static (a, b) => string.Compare(a._sortKey, b._sortKey, StringComparison.Ordinal));
        }
    }

    public void Dispose()
    {
        lock (Reporters)
        {
            Reporters.Remove(this);
        }
    }

    internal long ReportCommandStart()
    {
        CommandsExecuting.Add(1, _tags);
        return CommandDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
    }

    internal void ReportCommandStop(long startTimestamp)
    {
        CommandsExecuting.Add(-1, _tags);

        if (CommandDuration.Enabled && startTimestamp > 0)
        {
            CommandDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, _tags);
        }
    }

    internal void ReportCommandFailed(StatusCode statusCode)
    {
        var tags = _tags;
        tags.Add("ydb.status_code", statusCode.ToString());
        CommandsFailed.Add(1, tags);
    }

    private static IEnumerable<Measurement<int>> GetSessionCount()
    {
        lock (Reporters)
        {
            var measurements = new List<Measurement<int>>();

            foreach (var reporter in Reporters)
            {
                var (idle, busy) = reporter._sessionPool.Statistics;
                measurements.Add(MeasurementWithState(reporter, idle, "idle"));
                measurements.Add(MeasurementWithState(reporter, busy, "used"));
            }

            return measurements;
        }
    }

    private static Measurement<int> MeasurementWithState(YdbMetricsReporter reporter, int value, string state)
    {
        var tags = new KeyValuePair<string, object?>[reporter._tagsForMeasurement.Length + 1];
        reporter._tagsForMeasurement.AsSpan().CopyTo(tags);
        tags[^1] = new KeyValuePair<string, object?>("db.client.connection.state", state);
        return new Measurement<int>(value, tags);
    }
}
