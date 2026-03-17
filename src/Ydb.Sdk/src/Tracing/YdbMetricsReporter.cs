namespace Ydb.Sdk.Tracing;

using System.Diagnostics;
using System.Diagnostics.Metrics;

public sealed class YdbMetricsReporter : IDisposable
{
    const string Version = "0.1.0";
    static readonly Meter Meter;

    static readonly Histogram<double> CommandDuration;
    static readonly Counter<int> CommandsFailed;
    
    static readonly UpDownCounter<int> CommandsExecuting;
    
    static readonly List<YdbMetricsReporter> Reporters = [];

    readonly DriverConfig _config;
    readonly TagList _commonTags;
    readonly KeyValuePair<string, object?> _poolTag;

    static readonly InstrumentAdvice<double> ShortHistogramAdvice = new()
    {
        HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
    };
    
    internal ISessionPoolStats? StatsProvider { get; set; }

    static YdbMetricsReporter()
    {
        Meter = new("Ydb.Sdk", Version);

        CommandDuration = Meter.CreateHistogram<double>(
            "db.client.operation.duration",
            unit: "s",
            description: "Duration of database client operations.",
            advice: ShortHistogramAdvice);

        CommandsFailed = Meter.CreateCounter<int>(
            "db.client.operation.failed",
            unit: "{command}",
            description: "The number of database commands which have failed.");

        CommandsExecuting = Meter.CreateUpDownCounter<int>(
            "db.client.operation.ydb.executing",
            unit: "{command}",
            description: "The number of currently executing YDB commands.");

        Meter.CreateObservableUpDownCounter(
            "db.client.connection.count",
            GetSessionCount,
            unit: "{connection}",
            description: "The number of connections that are currently in state described by the state attribute.");
    }

    public YdbMetricsReporter(DriverConfig config)
    {
        _config = config;
        _poolTag = new KeyValuePair<string, object?>("db.client.connection.pool.name", config.Database);

        _commonTags = new TagList
        {
            { "db.system.name", "ydb" }, 
            { "db.namespace", config.Database },
            { "server.address", config.EndpointInfo.Host },
            { "server.port", config.EndpointInfo.Port }
        };

        lock (Reporters)
        {
            Reporters.Add(this);
        }
    }

    internal long ReportCommandStart()
    {
        CommandsExecuting.Add(1, _commonTags);
        return CommandDuration.Enabled ? Stopwatch.GetTimestamp() : 0;
    }

    internal void ReportCommandStop(long startTimestamp)
    {
        CommandsExecuting.Add(-1, _commonTags);
        if (CommandDuration.Enabled && startTimestamp > 0)
        {
            double seconds;

            var elapsedTicks = Stopwatch.GetTimestamp() - startTimestamp;
            seconds = (double)elapsedTicks / Stopwatch.Frequency;

            CommandDuration.Record(seconds, _commonTags);
        }
    }

    internal void ReportCommandFailed() => CommandsFailed.Add(1, _commonTags);
    
    static IEnumerable<Measurement<int>> GetSessionCount()
    {
        lock (Reporters)
        {
            var measurements = new List<Measurement<int>>();

            foreach (var reporter in Reporters)
            {
                if (reporter.StatsProvider == null) continue;
                measurements.Add(new Measurement<int>(
                    reporter.StatsProvider.IdleCount,
                    [
                        .. reporter._commonTags,
                        new KeyValuePair<string, object?>("db.client.connection.state", "idle")
                    ]
                ));

                measurements.Add(new Measurement<int>(
                    reporter.StatsProvider.BusyCount,
                    [
                        .. reporter._commonTags,
                        new KeyValuePair<string, object?>("db.client.connection.state", "used")
                    ]
                ));
            }

            return measurements;
        }
    }

    public void Dispose()
    {
        lock (Reporters)
        {
            Reporters.Remove(this);
        }
    }
}

internal interface ISessionPoolStats
{
    int IdleCount { get; }
    int BusyCount { get; }
}
