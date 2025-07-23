using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Stress.Loader;

public class LoadPattern(StressConfig config, ILogger logger)
{
    public async IAsyncEnumerable<int> GetLoadStepsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var totalDurationMs = config.TotalTestTimeSeconds * 1000;
        var elapsed = 0;

        while (elapsed < totalDurationMs && !cancellationToken.IsCancellationRequested)
        {
            logger.LogInformation("[{Now}]: Peak load phase! Expected RPS: {PickRps}", DateTime.Now, config.PeakRps);
            yield return config.PeakRps;
            var peakDurationMs = Math.Min(config.PeakDurationSeconds * 1000, totalDurationMs - elapsed);
            await Task.Delay(peakDurationMs, cancellationToken);
            elapsed += peakDurationMs;

            if (elapsed >= totalDurationMs)
            {
                break;
            }

            logger.LogInformation("[{Now}]: Medium load phase (after peak)! Expected RPS: {MediumRps}", DateTime.Now,
                config.MediumRps);
            yield return config.MediumRps;
            var mediumDurationMs = Math.Min(config.MediumDurationSeconds * 1000, totalDurationMs - elapsed);
            await Task.Delay(mediumDurationMs, cancellationToken);
            elapsed += mediumDurationMs;

            if (elapsed >= totalDurationMs)
            {
                break;
            }

            logger.LogInformation("[{Now}]: Minimum load phase! Expected RPS: {MediumRps}", DateTime.Now,
                config.MinRps);
            yield return config.MinRps;
            var minDurationMs = Math.Min(config.MinDurationSeconds * 1000, totalDurationMs - elapsed);
            await Task.Delay(minDurationMs, cancellationToken);
            elapsed += minDurationMs;

            if (elapsed >= totalDurationMs)
            {
                break;
            }

            logger.LogInformation("[{Now}]: Medium load phase (before next peak)! Expected RPS: {MediumRps}",
                DateTime.Now, config.MediumRps);
            yield return config.MediumRps;
            var finalMediumDurationMs = Math.Min(config.MediumDurationSeconds * 1000, totalDurationMs - elapsed);
            await Task.Delay(finalMediumDurationMs, cancellationToken);
            elapsed += finalMediumDurationMs;
        }
    }
}
