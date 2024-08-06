using System.Diagnostics;
using Prometheus;
using Ydb.Sdk;

namespace Internal.Jobs;

public abstract class Job
{
    private readonly Gauge _inFlightGauge;

    private readonly Gauge _okGauge;
    private readonly Gauge _notOkGauge;

    private readonly Summary _latencySummary;

    private readonly RateLimitedCaller _rateLimitedCaller;
    protected readonly TimeSpan Timeout;

    protected readonly Histogram AttemptsHistogram;
    protected readonly Gauge ErrorsGauge;
    protected readonly Random Random = new();

    protected readonly Client Client;

    protected Job(Client client, RateLimitedCaller rateLimitedCaller, string jobName, TimeSpan timeout)
    {
        Client = client;
        _rateLimitedCaller = rateLimitedCaller;
        Timeout = timeout;

        var metricFactory = Metrics.WithLabels(new Dictionary<string, string>
        {
            { "jobName", jobName },
            { "sdk", "dotnet" },
            { "sdkVersion", Environment.Version.ToString() }
        });

        _okGauge = metricFactory.CreateGauge("oks", "Count of OK");
        _notOkGauge = metricFactory.CreateGauge("not_oks", "Count of not OK");
        _inFlightGauge = metricFactory.CreateGauge("in_flight", "amount of requests in flight");
        _latencySummary = metricFactory.CreateSummary("latency", "Latencies (OK)", ["status"],
            new SummaryConfiguration
            {
                MaxAge = TimeSpan.FromSeconds(15),
                Objectives = new QuantileEpsilonPair[]
                {
                    new(0.5, 0.05),
                    new(0.99, 0.005),
                    new(0.999, 0.0005)
                }
            });

        AttemptsHistogram = metricFactory.CreateHistogram("attempts", "summary of amount for request", ["status"],
            new HistogramConfiguration { Buckets = Histogram.LinearBuckets(1, 1, 10) });

        ErrorsGauge = metricFactory.CreateGauge("errors", "amount of errors", ["class", "in"]);

        foreach (var statusCode in Enum.GetValues<StatusCode>())
        {
            ErrorsGauge.WithLabels(Utils.GetResonseStatusName(statusCode), "retried").IncTo(0);
            ErrorsGauge.WithLabels(Utils.GetResonseStatusName(statusCode), "finally").IncTo(0);
        }
    }

    public async void Start()
    {
        await _rateLimitedCaller.StartCalling(
            () => Client.CallFuncWithSessionPoolLimit(
                async () => await DoJob()),
            _inFlightGauge);
    }

    private async Task DoJob()
    {
        _inFlightGauge.Inc();
        var sw = Stopwatch.StartNew();
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