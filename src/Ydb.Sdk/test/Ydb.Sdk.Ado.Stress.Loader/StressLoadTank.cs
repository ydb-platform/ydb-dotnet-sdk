using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Yc;

namespace Ydb.Sdk.Ado.Stress.Loader;

public class StressLoadTank
{
    private readonly StressConfig _config;
    private readonly ILogger<StressLoadTank> _logger;
    private readonly YdbConnectionStringBuilder _settings;

    public StressLoadTank(StressConfig config)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

        _config = config;
        _logger = loggerFactory.CreateLogger<StressLoadTank>();
        _settings = new YdbConnectionStringBuilder(config.ConnectionString)
        {
            LoggerFactory = loggerFactory,
            CredentialsProvider = config.SaFilePath != null
                ? new ServiceAccountProvider(config.SaFilePath, loggerFactory)
                : new MetadataProvider(loggerFactory),
            ServerCertificates = YcCerts.GetYcServerCertificates()
        };

        ValidateConfig();
    }

    public async Task Run()
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
        var rpsControllerTask = Task.Run(async () =>
        {
            // ReSharper disable once PossiblyMistakenUseOfCancellationToken
            await foreach (var targetRps in loadPattern.GetLoadStepsAsync(cancellationToken))
            {
                await currentRpsSource.CancelAsync();

                currentRpsSource = new CancellationTokenSource();
                var combinedToken = CancellationTokenSource
                    .CreateLinkedTokenSource(cancellationToken, currentRpsSource.Token).Token;
                _ = StartWorkersForRpsAsync(targetRps, combinedToken);
            }
            // ReSharper disable once PossiblyMistakenUseOfCancellationToken
        }, cancellationToken);

        try
        {
            await rpsControllerTask;
        }
        finally
        {
            await currentRpsSource.CancelAsync();
        }
    }

    private async Task StartWorkersForRpsAsync(int targetRps, CancellationToken cancellationToken)
    {
        if (targetRps <= 0) return;

        targetRps = targetRps / 10 + targetRps % 10;

        await using var rateLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(100),
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

            _ = Task.Run(async () =>
            {
                try
                {
                    await using var ydbConnection = new YdbConnection(_settings);
                    await ydbConnection.OpenAsync(cancellationToken);
                    await new YdbCommand(ydbConnection) { CommandText = _config.TestQuery }
                        .ExecuteNonQueryAsync(cancellationToken);
                }
                catch (YdbException e)
                {
                    if (e.Code == StatusCode.ClientTransportTimeout)
                    {
                        return;
                    }

                    _logger.LogError(e, "Fail operation");
                }
            }, cancellationToken);
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
