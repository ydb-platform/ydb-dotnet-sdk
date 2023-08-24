using System.Diagnostics;
using Prometheus;
using Ydb.Sdk.Value;

namespace slo;

public abstract class Job
{
    protected readonly Random Random = new();

    protected readonly Table Table;
    private readonly RateLimitedCaller _rateLimitedCaller;

    private readonly Counter _okCounter;
    private readonly Counter _notOkCounter;

    private readonly Gauge _inFlightGauge;

    private readonly Summary _latencySummary;

    protected readonly Histogram AttemptsHistogram;

    protected Job(Table table, RateLimitedCaller rateLimitedCaller, string jobName)
    {
        Table = table;
        _rateLimitedCaller = rateLimitedCaller;

        var metricFactory = Metrics.WithLabels(new Dictionary<string, string>
        {
            { "jobName", jobName },
            { "sdk", "dotnet" },
            { "sdkVersion", Environment.Version.ToString() }
        });

        _okCounter = metricFactory.CreateCounter("oks", "Count of OK");
        _notOkCounter = metricFactory.CreateCounter("not_oks", "Count of not OK");
        _inFlightGauge = metricFactory.CreateGauge("in_flight", "amount of requests in flight");

        _latencySummary = metricFactory.CreateSummary(
            name: "latency",
            help: "Latencies (OK)",
            labelNames: new[] { "status" },
            new SummaryConfiguration
            {
                MaxAge = TimeSpan.FromSeconds(15),
                Objectives = new QuantileEpsilonPair[]
                {
                    new(0.5, 0.05),
                    new(0.99, 0.005),
                    new(1, 0.0005),
                }
            }
        );

        AttemptsHistogram = metricFactory.CreateHistogram(
            name: "attempts",
            help: "summary of amount for request",
            labelNames: new[] { "status" },
            new HistogramConfiguration { Buckets = Histogram.LinearBuckets(1, 1, 10) });
    }

    public async void Start()
    {
        await _rateLimitedCaller.StartCalling(async () => await DoJob(), _inFlightGauge);
    }

    private async Task DoJob()
    {
        var sw = Stopwatch.StartNew();
        _inFlightGauge.Inc();
        try
        {
            await PerformQuery();
            sw.Stop();

            _latencySummary.WithLabels("ok").Observe(sw.ElapsedMilliseconds);
            _okCounter.Inc();
            _inFlightGauge.Dec();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            sw.Stop();

            _latencySummary.WithLabels("err").Observe(sw.ElapsedMilliseconds);
            _notOkCounter.Inc();
            _inFlightGauge.Dec();
            throw;
        }
    }

    protected abstract Task PerformQuery();
}

internal class ReadJob : Job
{
    public ReadJob(Table table, RateLimitedCaller rateLimitedCaller) : base(table, rateLimitedCaller, "read")
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64((ulong)Random.Next(DataGenerator.MaxId)) }
        };

        await Table.Executor.ExecuteDataQuery(
            Queries.GetReadQuery(Table.TableName),
            parameters,
            AttemptsHistogram
        );
        // Console.WriteLine("read");
    }
}

internal class WriteJob : Job
{
    public WriteJob(Table table, RateLimitedCaller rateLimitedCaller) : base(table, rateLimitedCaller, "write")
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = DataGenerator.GetUpsertData();

        await Table.Executor.ExecuteDataQuery(
            Queries.GetWriteQuery(Table.TableName),
            parameters,
            AttemptsHistogram
        );
        // Console.WriteLine("write");
    }
}