using System.Diagnostics;
using Prometheus;

namespace slo.Jobs;

public abstract class Job
{
    private readonly Gauge _inFlightGauge;

    private readonly Gauge _okGauge;
    private readonly Gauge _notOkGauge;

    private readonly Summary _latencySummary;

    private readonly RateLimitedCaller _rateLimitedCaller;

    protected readonly Histogram AttemptsHistogram;
    protected readonly Random Random = new();

    protected readonly Table Table;

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

        _okGauge = metricFactory.CreateGauge("oks", "Count of OK");
        _notOkGauge = metricFactory.CreateGauge("not_oks", "Count of not OK");
        _inFlightGauge = metricFactory.CreateGauge("in_flight", "amount of requests in flight");

        _latencySummary = metricFactory.CreateSummary(
            "latency",
            "Latencies (OK)",
            new[] { "status" },
            new SummaryConfiguration
            {
                MaxAge = TimeSpan.FromSeconds(15),
                Objectives = new QuantileEpsilonPair[]
                {
                    new(0.5, 0.05),
                    new(0.99, 0.005),
                    new(1, 0.0005)
                }
            }
        );

        AttemptsHistogram = metricFactory.CreateHistogram(
            "attempts",
            "summary of amount for request",
            new[] { "status" },
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
            _okGauge.Inc();
            _inFlightGauge.Dec();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            sw.Stop();

            _latencySummary.WithLabels("err").Observe(sw.ElapsedMilliseconds);
            _notOkGauge.Inc();
            _inFlightGauge.Dec();
            throw;
        }
    }

    protected abstract Task PerformQuery();
}