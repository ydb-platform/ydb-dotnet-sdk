using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Yc;

namespace Ydb.Sdk.Ado.Stress.Loader;

public class StressTestTank
{
    private readonly StressTestConfig _config;
    private readonly ILogger<StressTestTank> _logger;
    private readonly YdbConnectionStringBuilder _settings;

    public StressTestTank(StressTestConfig config)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        _config = config;
        _logger = loggerFactory.CreateLogger<StressTestTank>();
        _settings = new YdbConnectionStringBuilder(config.ConnectionString)
        {
            LoggerFactory = loggerFactory,
            CredentialsProvider =
                config.SaFilePath != null ? new ServiceAccountProvider(config.SaFilePath, loggerFactory) : null
        };

        ValidateConfig();
    }

    public async Task RunAsync()
    {
        _logger.LogInformation(
            """
            Starting YDB ADO.NET Stress Test Tank
            Configuration:
                Peak RPS: {PeakRps}
                Medium RPS: {MediumRps}
                Min RPS: {MinRps}
                Load Pattern: Peak({PeakDuration}s) -> Medium({MediumDuration}s) -> Min({MinDuration}s) -> Medium({MediumDuration}s)
                Total Test Time: {TotalTime}s
                Test Query: {TestQuery}
            """,
            _config.PeakRps, _config.MediumRps, _config.MinRps, _config.PeakDurationSeconds,
            _config.MediumDurationSeconds, _config.MinDurationSeconds, _config.MediumDurationSeconds,
            _config.TotalTestTimeSeconds, _config.TestQuery
        );

        var ctsRunJob = new CancellationTokenSource();
        var loadPattern = new LoadPattern(_config, _logger);
        try
        {
            ctsRunJob.CancelAfter(TimeSpan.FromSeconds(_config.TotalTestTimeSeconds));

            await RunLoadPatternAsync(loadPattern, ctsRunJob.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("⏰ Test completed after {TotalTime}s", _config.TotalTestTimeSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "❌ Test failed with error");

            throw;
        }
    }

    private async Task RunLoadPatternAsync(LoadPattern loadPattern, CancellationToken cancellationToken)
    {
        var currentRpsSource = new CancellationTokenSource();
        var workerTasks = new List<Task>();

        // Start background task to update RPS based on load pattern
        var rpsControllerTask = Task.Run(async () =>
        {
            await foreach (var targetRps in loadPattern.GetLoadStepsAsync(cancellationToken))
            {
                await currentRpsSource.CancelAsync();
                await Task.WhenAll(workerTasks);
                workerTasks.Clear();

                currentRpsSource = new CancellationTokenSource();
                var combinedToken = CancellationTokenSource
                    .CreateLinkedTokenSource(cancellationToken, currentRpsSource.Token).Token;
                await StartWorkersForRpsAsync(targetRps, workerTasks, combinedToken);
            }
        }, cancellationToken);

        try
        {
            await rpsControllerTask;
        }
        finally
        {
            await currentRpsSource.CancelAsync();
            await Task.WhenAll(workerTasks);
        }
    }

    private async Task StartWorkersForRpsAsync(int targetRps, List<Task> workerTasks,
        CancellationToken cancellationToken)
    {
        if (targetRps <= 0) return;

        await using var rateLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromSeconds(1),
            PermitLimit = targetRps,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            QueueLimit = targetRps * 2
        });

        _logger.LogInformation("Starting shooting for {TargetRps} RPS", targetRps);

        while (!cancellationToken.IsCancellationRequested)
        {
            using var lease = await rateLimiter.AcquireAsync(1, cancellationToken);

            if (!lease.IsAcquired)
            {
                continue;
            }

            workerTasks.Add(
                Task.Run(async () =>
                {
                    try
                    {
                        await using var ydbConnection = new YdbConnection(_settings);
                        await ydbConnection.OpenAsync(cancellationToken);
                        await new YdbCommand(ydbConnection) { CommandText = _config.TestQuery }
                            .ExecuteNonQueryAsync(cancellationToken);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Fail operation");
                    }
                }, cancellationToken)
            );
        }
    }

    private void ValidateConfig()
    {
        if (_config.PeakRps < _config.MediumRps)
        {
            throw new ArgumentException("Peak RPS must be greater than Medium RPS");
        }

        if (_config.MediumRps < _config.MinRps)
        {
            throw new ArgumentException("Medium RPS must be greater than Min RPS");
        }

        if (_config.MinRps < 1)
        {
            throw new ArgumentException("Min RPS must be at least 1");
        }

        if (_config.PeakDurationSeconds < 1 || _config.MediumDurationSeconds < 1 || _config.MinDurationSeconds < 1)
        {
            throw new ArgumentException("All duration settings must be at least 1 second");
        }

        if (string.IsNullOrWhiteSpace(_config.TestQuery))
        {
            throw new ArgumentException("Test query cannot be empty");
        }
    }
}
