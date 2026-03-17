using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ydb.Sdk.Tracing;

public sealed class YdbMetricsReporter : IDisposable
{
    private const string Version = "0.1.0";

    private static readonly Histogram<double> CommandDuration;
    private static readonly Counter<int> CommandsFailed;

    private static readonly UpDownCounter<int> CommandsExecuting;

    private static readonly List<YdbMetricsReporter> Reporters = [];

    private readonly TagList _commonTags;

    static YdbMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk", Version);

        var shortHistogramAdvice = new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
        };

        CommandDuration = meter.CreateHistogram(
            "db.client.operation.duration",
            "s",
            "Duration of database client operations.",
            advice: shortHistogramAdvice);

        CommandsFailed = meter.CreateCounter<int>(
            "db.client.operation.failed",
            "{command}",
            "The number of database commands which have failed.");

        CommandsExecuting = meter.CreateUpDownCounter<int>(
            "db.client.operation.ydb.executing",
            "{command}",
            "The number of currently executing YDB commands.");

        meter.CreateObservableUpDownCounter(
            "db.client.connection.count",
            GetSessionCount,
            "{connection}",
            "The number of connections that are currently in state described by the state attribute.");
    }

    public YdbMetricsReporter(DriverConfig config)
    {
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

    internal ISessionPoolStats? StatsProvider { get; set; }

    public void Dispose()
    {
        lock (Reporters)
        {
            Reporters.Remove(this);
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

    private static IEnumerable<Measurement<int>> GetSessionCount()
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
}

internal interface ISessionPoolStats
{
    int IdleCount { get; }
    int BusyCount { get; }
}
