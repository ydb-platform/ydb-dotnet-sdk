using Prometheus;
using slo.Jobs;
using Ydb.Sdk;
using Ydb.Sdk.Table;

namespace slo.Cli;

public static class CliCommands
{
    internal static async Task Create(CreateConfig createConfig)
    {
        Console.WriteLine(createConfig);
        var config = new DriverConfig(
            createConfig.Endpoint,
            createConfig.Db
        );

        await using var driver = await Driver.CreateInitialized(config);

        using var tableClient = new TableClient(driver);

        var executor = new Executor(tableClient);

        var table = new Table(createConfig.TableName, executor);
        await table.Init(createConfig.InitialDataCount, createConfig.PartitionSize, createConfig.MinPartitionsCount,
            createConfig.MaxPartitionsCount);
    }

    internal static async Task CleanUp(CleanUpConfig cleanUpConfig)
    {
        Console.WriteLine(cleanUpConfig);
        var config = new DriverConfig(
            cleanUpConfig.Endpoint,
            cleanUpConfig.Db
        );

        await using var driver = await Driver.CreateInitialized(config);

        using var tableClient = new TableClient(driver);

        var executor = new Executor(tableClient);

        var table = new Table(cleanUpConfig.TableName, executor);
        await table.CleanUp();
    }

    internal static async Task Run(RunConfig runConfig)
    {
        var config = new DriverConfig(
            runConfig.Endpoint,
            runConfig.Db
        );

        await using var driver = await Driver.CreateInitialized(config);

        using var tableClient = new TableClient(driver);

        var executor = new Executor(tableClient);

        var table = new Table(runConfig.TableName, executor);
        await table.Init(runConfig.InitialDataCount, 1, 6, 1000);

        Console.WriteLine(runConfig.PromPgw);
        
        using var prometheus = new MetricPusher(endpoint: runConfig.PromPgw, job: "slo");

        prometheus.Start();

        var duration = TimeSpan.FromSeconds(runConfig.Time);

        var readJob = new ReadJob(table, new RateLimitedCaller(
            runConfig.ReadRps,
            duration
        ));

        var writeJob = new WriteJob(table, new RateLimitedCaller(
            runConfig.WriteRps,
            duration
        ));

        var readThread = new Thread(readJob.Start);
        var writeThread = new Thread(writeJob.Start);

        readThread.Start();
        writeThread.Start();

        await Task.Delay(duration + TimeSpan.FromSeconds(runConfig.ShutdownTime));

        readThread.Join();
        writeThread.Join();
    }
}