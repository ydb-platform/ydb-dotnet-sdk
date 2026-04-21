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
    private static readonly InstrumentAdvice<double> ShortHistogramAdvice = new()
        { HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10] };

    // Operation metrics: duration and failures
    private static readonly Histogram<double> OperationDuration;
    private static readonly Counter<int> OperationsFailed;

    // Pool metrics: connection lifecycle (count, timeouts, pending requests, create time)
    // create_time covers CreateSession RPC + first AttachStream message
    private static readonly Counter<int> ConnectionTimeouts;
    private static readonly UpDownCounter<int> PendingConnectionRequests;
    private static readonly Histogram<double> ConnectionCreateTime;

    private static readonly List<YdbMetricsReporter> Reporters = [];

    private readonly TagList _operationMetricTags;
    private readonly KeyValuePair<string, object?> _poolNameTag;
    private readonly string _sortKey;

    private readonly Func<(int Idle, int Busy)> _statisticsProvider = null!;
    private readonly int _maxPoolSize;
    private readonly int _minPoolSize;

    static YdbMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk", YdbSdkVersion.Value);

        OperationDuration = meter.CreateHistogram(
            "db.client.operation.duration",
            unit: "s",
            description: "Duration of database client operations.",
            advice: ShortHistogramAdvice);

        meter.CreateObservableUpDownCounter(
            "ydb.query.session.count",
            GetSessionCount,
            unit: "{session}",
            description: "The number of sessions that are currently in state described by the state attribute.");

        meter.CreateObservableGauge(
            "ydb.query.session.max",
            GetSessionMax,
            unit: "{session}",
            description: "The maximum number of open sessions allowed in the pool.");

        meter.CreateObservableGauge(
            "ydb.query.session.min",
            GetSessionMin,
            unit: "{session}",
            description: "The minimum number of idle open sessions allowed in the pool.");

        OperationsFailed = meter.CreateCounter<int>(
            "ydb.client.operation.failed",
            unit: "{command}",
            description: "The number of database commands which have failed.");

        ConnectionTimeouts = meter.CreateCounter<int>(
            "ydb.query.session.timeouts",
            unit: "{timeout}",
            description:
            "The number of times a session could not be acquired from the pool before the timeout elapsed.");

        PendingConnectionRequests = meter.CreateUpDownCounter<int>(
            "ydb.query.session.pending_requests",
            unit: "{request}",
            description: "The number of pending requests for an open connection, cumulative for the entire pool.");

        ConnectionCreateTime = meter.CreateHistogram(
            "ydb.query.session.create_time",
            unit: "s",
            description: "The time it took to create a new connection (CreateSession + first AttachStream message).",
            advice: ShortHistogramAdvice);
    }

    internal YdbMetricsReporter(YdbConnectionStringBuilder settings)
    {
        (_operationMetricTags, _poolNameTag, _sortKey) = BuildTags(settings);
    }

    internal YdbMetricsReporter(
        int maxPoolSize,
        int minPoolSize,
        Func<(int Idle, int Busy)> statisticsProvider,
        YdbConnectionStringBuilder settings)
    {
        _maxPoolSize = maxPoolSize;
        _minPoolSize = minPoolSize;
        _statisticsProvider = statisticsProvider;
        (_operationMetricTags, _poolNameTag, _sortKey) = BuildTags(settings);
        Register();
    }

    private static (TagList, KeyValuePair<string, object?>, string) BuildTags(YdbConnectionStringBuilder settings) => (
        new TagList
        {
            { "db.system.name", "ydb" },
            { "db.namespace", settings.Database },
            { "server.address", settings.Host },
            { "server.port", settings.Port }
        },
        new KeyValuePair<string, object?>("ydb.query.session.pool.name",
            settings.PoolName ?? settings.ConnectionString),
        settings.ConnectionString
    );

    private void Register()
    {
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

    internal static long ReportCommandStart() => OperationDuration.Enabled ? Stopwatch.GetTimestamp() : 0;

    internal void ReportCommandStop(long startTimestamp, string operationName)
    {
        if (OperationDuration.Enabled && startTimestamp > 0)
        {
            var durationMetricTags = _operationMetricTags;
            durationMetricTags.Add("ydb.operation.name", operationName);
            OperationDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, durationMetricTags);
        }
    }

    internal void ReportConnectionTimeout() => ConnectionTimeouts.Add(1, _poolNameTag);

    internal void ReportOperationFailed(StatusCode statusCode, string operationName)
    {
        var tags = _operationMetricTags;
        tags.Add("ydb.operation.name", operationName);
        tags.Add("db.response.status_code", statusCode.ToString());
        OperationsFailed.Add(1, tags);
    }

    internal void ReportPendingConnectionRequestStart() => PendingConnectionRequests.Add(1, _poolNameTag);

    internal void ReportPendingConnectionRequestStop() => PendingConnectionRequests.Add(-1, _poolNameTag);

    internal static long ReportConnectionCreateTimeStart() =>
        ConnectionCreateTime.Enabled ? Stopwatch.GetTimestamp() : 0;

    internal void ReportConnectionCreateTime(long startTimestamp)
    {
        if (startTimestamp > 0)
            ConnectionCreateTime.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, _poolNameTag);
    }

    private static IEnumerable<Measurement<int>> GetSessionCount()
    {
        lock (Reporters)
        {
            var measurements = new List<Measurement<int>>();

            foreach (var reporter in Reporters)
            {
                var (idle, busy) = reporter._statisticsProvider();
                measurements.Add(new Measurement<int>(
                    idle,
                    reporter._poolNameTag,
                    new KeyValuePair<string, object?>("ydb.query.session.state", "idle")));

                measurements.Add(new Measurement<int>(
                    busy,
                    reporter._poolNameTag,
                    new KeyValuePair<string, object?>("ydb.query.session.state", "used")));
            }

            return measurements;
        }
    }

    private static IEnumerable<Measurement<int>> GetSessionMax()
    {
        lock (Reporters)
        {
            return Reporters.Select(reporter => new Measurement<int>(reporter._maxPoolSize, reporter._poolNameTag))
                .ToList();
        }
    }

    private static IEnumerable<Measurement<int>> GetSessionMin()
    {
        lock (Reporters)
        {
            return Reporters.Select(reporter => new Measurement<int>(reporter._minPoolSize, reporter._poolNameTag))
                .ToList();
        }
    }
}
