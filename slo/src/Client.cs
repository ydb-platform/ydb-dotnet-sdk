using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk;
using Ydb.Sdk.Table;

namespace slo;

public class Client : IAsyncDisposable
{
    public readonly Executor Executor;
    public readonly string TableName;

    private readonly ServiceProvider _serviceProvider;
    private readonly Driver _driver;
    private readonly TableClient _tableClient;

    private readonly Semaphore _semaphore;

    private Client(string tableName, Executor executor, ServiceProvider serviceProvider, Driver driver,
        TableClient tableClient, uint sessionPoolLimit)
    {
        TableName = tableName;
        Executor = executor;
        _serviceProvider = serviceProvider;
        _driver = driver;
        _tableClient = tableClient;
        _semaphore = new Semaphore((int)sessionPoolLimit - 1, (int)sessionPoolLimit - 1);
    }

    public async Task Init(int initialDataCount, int partitionSize, int minPartitionsCount, int maxPartitionsCount,
        TimeSpan timeout)
    {
        await Executor.ExecuteSchemeQuery(
            Queries.GetCreateQuery(TableName, partitionSize, minPartitionsCount, maxPartitionsCount),
            timeout);

        await DataGenerator.LoadMaxId(TableName, Executor);

        var tasks = new List<Task> { Capacity = initialDataCount };

        for (var i = 0; i < initialDataCount; i++)
        {
            await CallFuncWithSessionPoolLimit(() => Executor.ExecuteDataQuery(
                Queries.GetWriteQuery(TableName),
                DataGenerator.GetUpsertData(),
                timeout: timeout
            ));
        }

        await Task.WhenAll(tasks);
    }

    public async Task CleanUp(TimeSpan timeout)
    {
        await Executor.ExecuteSchemeQuery(Queries.GetDropQuery(TableName), timeout);
    }

    private static ServiceProvider GetServiceProvider()
    {
        return new ServiceCollection()
            .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Information))
            .BuildServiceProvider();
    }

    public static async Task<Client> CreateAsync(string endpoint, string db, string tableName,
        uint sessionPoolLimit = 100)
    {
        var driverConfig = new DriverConfig(
            endpoint,
            db
        );

        var serviceProvider = GetServiceProvider();
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

        loggerFactory ??= NullLoggerFactory.Instance;
        var driver = await Driver.CreateInitialized(driverConfig, loggerFactory);

        var tableClient = new TableClient(driver, new TableClientConfig(new SessionPoolConfig(sessionPoolLimit)));

        var executor = new Executor(tableClient);

        var table = new Client(tableName, executor, serviceProvider, driver, tableClient, sessionPoolLimit);

        return table;
    }

    public Task CallFuncWithSessionPoolLimit(Func<Task> func)
    {
        _semaphore.WaitOne();

        async Task FuncWithRelease()
        {
            try
            {
                await func();
            }
            finally
            {
                _semaphore.Release();
            }
        }

        _ = FuncWithRelease();
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _tableClient.Dispose();
        await _driver.DisposeAsync();
        await _serviceProvider.DisposeAsync();
    }
}