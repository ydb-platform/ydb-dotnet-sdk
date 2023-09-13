using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;
using slo.Jobs;
using Ydb.Sdk;
using Ydb.Sdk.Table;

namespace slo.Cli;

public static class CliCommands
{
    private static ServiceProvider GetServiceProvider()
    {
        return new ServiceCollection()
            .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Information))
            .BuildServiceProvider();
    }


    internal static async Task Create(CreateConfig config)
    {
        Console.WriteLine(config);
        var driverConfig = new DriverConfig(
            config.Endpoint,
            config.Db
        );

        await using var serviceProvider = GetServiceProvider();
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

        loggerFactory ??= NullLoggerFactory.Instance;
        await using var driver = await Driver.CreateInitialized(driverConfig, loggerFactory);

        using var tableClient = new TableClient(driver);

        var executor = new Executor(tableClient);

        var table = new Table(config.TableName, executor);
        const int maxCreateAttempts = 10;
        for (var i = 0; i < maxCreateAttempts; i++)
        {
            try
            {
                await table.Init(config.InitialDataCount,
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
        var driverConfig = new DriverConfig(
            config.Endpoint,
            config.Db
        );

        await using var serviceProvider = GetServiceProvider();
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

        loggerFactory ??= NullLoggerFactory.Instance;
        await using var driver = await Driver.CreateInitialized(driverConfig, loggerFactory);

        using var tableClient = new TableClient(driver);

        var executor = new Executor(tableClient);

        var table = new Table(config.TableName, executor);
        await table.CleanUp(TimeSpan.FromMilliseconds(config.WriteTimeout));
    }

    internal static async Task Run(RunConfig config)
    {
        var promPgwEndpoint = $"{config.PromPgw}/metrics";
        const string job = "workload-dotnet";

        var driverConfig = new DriverConfig(
            config.Endpoint,
            config.Db
        );

        await using var serviceProvider = GetServiceProvider();
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

        loggerFactory ??= NullLoggerFactory.Instance;
        await using var driver = await Driver.CreateInitialized(driverConfig, loggerFactory);

        using var tableClient = new TableClient(driver);

        var executor = new Executor(tableClient);

        var table = new Table(config.TableName, executor);
        await table.Init(config.InitialDataCount, 1, 6, 1000, TimeSpan.FromMilliseconds(config.WriteTimeout));

        Console.WriteLine(config.PromPgw);

        await MetricReset(promPgwEndpoint, job);
        using var prometheus = new MetricPusher(promPgwEndpoint, job, intervalMilliseconds: config.ReportPeriod);

        prometheus.Start();

        var duration = TimeSpan.FromSeconds(config.Time);

        var readJob = new ReadJob(
            table,
            new RateLimitedCaller(
                config.ReadRps,
                duration
            ),
            TimeSpan.FromMilliseconds(config.ReadTimeout));

        var writeJob = new WriteJob(
            table,
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