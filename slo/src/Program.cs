using slo;
using Ydb.Sdk;
using Ydb.Sdk.Table;
using Prometheus;

var config = new DriverConfig(
    endpoint: "http://localhost:2136",
    database: "/local"
);

await using var driver = await Driver.CreateInitialized(config);

using var tableClient = new TableClient(driver, new TableClientConfig());

var executor = new Executor(tableClient);

await using var table = await Table.Create("slo-test", executor);

// using var prometheus = new MetricServer(port: 1234);
using var prometheus = new MetricPusher(new MetricPusherOptions
{
    Endpoint = "http://localhost:9091/metrics",
    Job = "slo"
});

prometheus.Start();

TimeSpan duration = default;
// var duration = TimeSpan.FromSeconds(10);


var readJob = new ReadJob(table, new RateLimitedCaller(
    rate: 1000,
    duration: duration
));

var writeJob = new WriteJob(table, new RateLimitedCaller(
    rate: 100,
    duration: duration
));

var readThread = new Thread(readJob.Start);
var writeThread = new Thread(writeJob.Start);

readThread.Start();
writeThread.Start();

Console.ReadLine();