using Ydb.Sdk.Ado.Retry.Delay;

namespace Ydb.Sdk.Ado.Retry;

public sealed class RetryConfig
{
    public int MaxAttempts { get; set; } = 10;

    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    public double Exponent { get; set; } = 2.0;

    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(10);

    public double JitterFraction { get; set; } = 0.5;

    public Dictionary<StatusCode, Func<int, TimeSpan?>> PerStatusDelay { get; } = new();

    public IStatusDelayProfile StatusDelayProfile { get; set; } = new DefaultStatusDelayProfile();

    public Func<Exception, int, TimeSpan?> DefaultDelay { get; set; } =
        static (_, attempt) =>
        {
            var baseMs = 100.0 * Math.Pow(2.0, Math.Max(0, attempt - 1));
            var jitter = 1.0 + (Random.Shared.NextDouble() * 0.5);
            var ms = Math.Min(baseMs * jitter, 10_000.0);
            return TimeSpan.FromMilliseconds(ms);
        };
}
