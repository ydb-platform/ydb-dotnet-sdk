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

    // Operation metrics: duration, failures, and in-flight command count
    private static readonly Histogram<double> OperationDuration;
    private static readonly Counter<int> OperationsFailed;
    private static readonly UpDownCounter<int> CommandsExecuting;

    // Pool metrics: connection lifecycle (count, timeouts, pending requests, create time)
    // create_time covers CreateSession RPC + first AttachStream message
    private static readonly Counter<int> ConnectionTimeouts;
    private static readonly UpDownCounter<int> PendingConnectionRequests;
    private static readonly Histogram<double> ConnectionCreateTime;

    private static readonly List<YdbMetricsReporter> Reporters = [];

    private readonly TagList _operationMetricTags;
    private readonly KeyValuePair<string, object?> _poolNameTag;
    private readonly string _sortKey;
    private readonly ISessionSource _sessionPool;

    static YdbMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk", YdbSdkVersion.Value);

        OperationDuration = meter.CreateHistogram(
            "db.client.operation.duration",
            unit: "s",
            description: "Duration of database client operations.",
            advice: ShortHistogramAdvice);

        meter.CreateObservableUpDownCounter(
            "db.client.connection.count",
            GetSessionCount,
            unit: "{connection}",
            description: "The number of connections that are currently in state described by the state attribute.");

        OperationsFailed = meter.CreateCounter<int>(
            "db.client.operation.failed",
            unit: "{command}",
            description: "The number of database commands which have failed.");

        CommandsExecuting = meter.CreateUpDownCounter<int>(
            "db.client.operation.ydb.executing_query",
            unit: "{command}",
            description: "The number of currently executing YDB commands.");

        ConnectionTimeouts = meter.CreateCounter<int>(
            "db.client.connection.timeouts",
            unit: "{connection}",
            description:
            "The number of times a connection could not be acquired from the pool before the timeout elapsed.");

        PendingConnectionRequests = meter.CreateUpDownCounter<int>(
            "db.client.connection.pending_requests",
            unit: "{request}",
            description: "The number of pending requests for an open connection, cumulative for the entire pool.");

        ConnectionCreateTime = meter.CreateHistogram(
            "db.client.connection.create_time",
            unit: "s",
            description: "The time it took to create a new connection (CreateSession + first AttachStream message).",
            advice: ShortHistogramAdvice);

    }

    internal YdbMetricsReporter(ISessionSource sessionPool, YdbConnectionStringBuilder settings)
    {
        _sessionPool = sessionPool;

        _operationMetricTags = new TagList
        {
            { "db.system.name", "ydb" },
            { "db.namespace", settings.Database },
            { "server.address", settings.Host },
            { "server.port", settings.Port }
        };

        _poolNameTag = new KeyValuePair<string, object?>("db.client.connection.pool.name",
            settings.PoolName ?? settings.ConnectionString);

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
        CommandsExecuting.Add(1, _poolNameTag);
        return OperationDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
    }

    internal void ReportCommandStop(long startTimestamp, string operationName)
    {
        CommandsExecuting.Add(-1, _poolNameTag);

        if (OperationDuration.Enabled && startTimestamp > 0)
        {
            var durationMetricTags = _operationMetricTags;
            durationMetricTags.Add("db.operation.name", operationName);
            OperationDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, durationMetricTags);
        }
    }

    internal void ReportConnectionTimeout() => ConnectionTimeouts.Add(1, _poolNameTag);

    internal void ReportOperationFailed(StatusCode statusCode, string operationName)
    {
        var tags = _operationMetricTags;
        tags.Add("db.operation.name", operationName);
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
                var (idle, busy) = reporter._sessionPool.Statistics;
                measurements.Add(new Measurement<int>(
                    idle,
                    reporter._poolNameTag,
                    new KeyValuePair<string, object?>("db.client.connection.state", "idle")));

                measurements.Add(new Measurement<int>(
                    busy,
                    reporter._poolNameTag,
                    new KeyValuePair<string, object?>("db.client.connection.state", "used")));
            }

            return measurements;
        }
    }
}
