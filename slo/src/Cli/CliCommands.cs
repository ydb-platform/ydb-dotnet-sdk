using Prometheus;
using slo.Jobs;

namespace slo.Cli;

public static class CliCommands
{
    internal static async Task Create(CreateConfig config)
    {
        Console.WriteLine(config);

        await using var client = await Client.CreateAsync(config.Endpoint, config.Db, config.TableName);

        const int maxCreateAttempts = 10;
        for (var i = 0; i < maxCreateAttempts; i++)
        {
            try
            {
                await client.Init(config.InitialDataCount,
                    config.PartitionSize,
                    config.MinPartitionsCount,
                    config.MaxPartitionsCount,
                    TimeSpan.FromMilliseconds(config.WriteTimeout));
                break;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Thread.Sleep(millisecondsTimeout: 1000);
            }
        }
    }

    internal static async Task CleanUp(CleanUpConfig config)
    {
        Console.WriteLine(config);

        await using var client = await Client.CreateAsync(config.Endpoint, config.Db, config.TableName);

        await client.CleanUp(TimeSpan.FromMilliseconds(config.WriteTimeout));
    }

    internal static async Task Run(RunConfig config)
    {
        var promPgwEndpoint = $"{config.PromPgw}/metrics";
        const string job = "workload-dotnet";

        await using var client = await Client.CreateAsync(config.Endpoint, config.Db, config.TableName);

        Console.WriteLine(config.PromPgw);

        await MetricReset(promPgwEndpoint, job);
        using var prometheus = new MetricPusher(promPgwEndpoint, job, intervalMilliseconds: config.ReportPeriod);

        prometheus.Start();

        var duration = TimeSpan.FromSeconds(config.Time);

        var readJob = new ReadJob(
            client,
            new RateLimitedCaller(
                config.ReadRps,
                duration
            ),
            TimeSpan.FromMilliseconds(config.ReadTimeout));

        var writeJob = new WriteJob(
            client,
            new RateLimitedCaller(
                config.WriteRps,
                duration
            ),
            TimeSpan.FromMilliseconds(config.WriteTimeout));

        var readThread = new Thread(readJob.Start);
        var writeThread = new Thread(writeJob.Start);

        readThread.Start();
        writeThread.Start();
        await Task.Delay(duration + TimeSpan.FromSeconds(config.ShutdownTime));
        readThread.Join();
        writeThread.Join();

        await prometheus.StopAsync();
        await MetricReset(promPgwEndpoint, job);
    }

    private static async Task MetricReset(string promPgwEndpoint, string job)
    {
        var deleteUri = $"{promPgwEndpoint}/job/{job}";
        using var httpClient = new HttpClient();
        await httpClient.DeleteAsync(deleteUri);
    }
}