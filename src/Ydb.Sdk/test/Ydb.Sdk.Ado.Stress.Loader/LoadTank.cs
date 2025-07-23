using System.Data;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Yc;

namespace Ydb.Sdk.Ado.Stress.Loader;

public class LoadTank
{
    private readonly ILogger<LoadTank> _logger;
    private readonly YdbConnectionStringBuilder _settings;
    private readonly LoadConfig _config;

    public LoadTank(LoadConfig config)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        _logger = loggerFactory.CreateLogger<LoadTank>();
        _settings = new YdbConnectionStringBuilder(config.ConnectionString)
        {
            CredentialsProvider = config.SaFilePath != null
                ? new ServiceAccountProvider(config.SaFilePath, loggerFactory)
                : new MetadataProvider(loggerFactory),
            ServerCertificates = YcCerts.GetYcServerCertificates()
        };
        _config = config;
    }

    public async Task Run()
    {
        _logger.LogInformation(
            """
            Starting YDB ADO.NET Stress Test Tank
            Configuration:
                Total Test Time: {TotalTime}s
                Test Query: {TestQuery}
            """,
            _config.TotalTestTimeSeconds,
            _config.TestQuery
        );

        _logger.LogInformation("[{Now}] Starting shooting with PoolingSessionSource...", DateTime.Now);
        var ctsStep1 = new CancellationTokenSource();
        var workers = new List<Task>();
        ctsStep1.CancelAfter(_config.TotalTestTimeSeconds * 500);
        
        for (var i = 0; i < _settings.MaxSessionPool; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                while (!ctsStep1.IsCancellationRequested)
                {
                    try
                    {
                        await using var ydbConnection = new YdbConnection(_settings);
                        await ydbConnection.OpenAsync(ctsStep1.Token);
                        await new YdbCommand(ydbConnection) { CommandText = _config.TestQuery }
                            .ExecuteNonQueryAsync(ctsStep1.Token);
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                }
            }, ctsStep1.Token));
        }

        await Task.WhenAll(workers);
        workers.Clear();
        _logger.LogInformation("Phase 1 stopped shooting");
        await Task.Delay(10_000);
        
        _logger.LogInformation("[{Now}] Starting shooting without PoolingSessionSource...", DateTime.Now);
        var ctsStep2 = new CancellationTokenSource();
        ctsStep2.CancelAfter(_config.TotalTestTimeSeconds * 500);
        for (var i = 0; i < _settings.MaxSessionPool; i++)
        {
            workers.Add(Task.Run(async () =>
            {
                await using var ydbConnection = new YdbConnection(_settings);

                while (!ctsStep2.IsCancellationRequested)
                {
                    try
                    {
                        if (ydbConnection.State != ConnectionState.Open)
                        {
                            await ydbConnection.OpenAsync(ctsStep2.Token);
                        }
                        
                        await new YdbCommand(ydbConnection) { CommandText = _config.TestQuery }
                            .ExecuteNonQueryAsync(ctsStep2.Token);
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                }
            }, ctsStep2.Token));
        }
        await Task.WhenAll(workers);
        _logger.LogInformation("Phase 2 stopped shooting");
    }
}
