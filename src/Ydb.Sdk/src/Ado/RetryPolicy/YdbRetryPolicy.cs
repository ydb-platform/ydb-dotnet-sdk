using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado.RetryPolicy;

/// <summary>
/// See <a href="https://aws.amazon.com/ru/blogs/architecture/exponential-backoff-and-jitter/">AWS paper</a>
/// </summary>
public class YdbRetryPolicy : IRetryPolicy
{
    public static readonly YdbRetryPolicy Default = new(YdbRetryPolicyConfig.Default);

    private readonly int _maxAttempt;
    private readonly int _fastBackoffBaseMs;
    private readonly int _slowBackoffBaseMs;
    private readonly int _fastCeiling;
    private readonly int _slowCeiling;
    private readonly int _fastCapBackoffMs;
    private readonly int _slowCapBackoffMs;
    private readonly bool _enableRetryIdempotence;
    private readonly IRandom _random;

    public YdbRetryPolicy(YdbRetryPolicyConfig config)
    {
        _maxAttempt = config.MaxAttempt;
        _fastBackoffBaseMs = config.FastBackoffBaseMs;
        _slowBackoffBaseMs = config.SlowBackoffBaseMs;
        _fastCeiling = (int)Math.Ceiling(Math.Log(config.FastCapBackoffMs + 1, 2));
        _slowCeiling = (int)Math.Ceiling(Math.Log(config.SlowCapBackoffMs + 1, 2));
        _fastCapBackoffMs = config.FastCapBackoffMs;
        _slowCapBackoffMs = config.SlowCapBackoffMs;
        _enableRetryIdempotence = config.EnableRetryIdempotence;
        _random = ThreadLocalRandom.Instance;
    }

    internal YdbRetryPolicy(YdbRetryPolicyConfig config, IRandom random) : this(config)
    {
        _random = random;
    }

    public TimeSpan? GetNextDelay(YdbException ydbException, int attempt)
    {
        if (attempt >= _maxAttempt || (!_enableRetryIdempotence && !ydbException.IsTransient))
            return null;

        return ydbException.Code switch
        {
            StatusCode.BadSession or StatusCode.SessionBusy => TimeSpan.Zero,
            StatusCode.Aborted or StatusCode.Undetermined =>
                FullJitter(_fastBackoffBaseMs, _fastCapBackoffMs, _fastCeiling, attempt, _random),
            StatusCode.Unavailable or StatusCode.ClientTransportUnknown or StatusCode.ClientTransportUnavailable =>
                EqualJitter(_fastBackoffBaseMs, _fastCapBackoffMs, _fastCeiling, attempt, _random),
            StatusCode.Overloaded or StatusCode.ClientTransportResourceExhausted =>
                EqualJitter(_slowBackoffBaseMs, _slowCapBackoffMs, _slowCeiling, attempt, _random),
            _ => null
        };
    }

    private static TimeSpan FullJitter(int backoffBaseMs, int capMs, int ceiling, int attempt, IRandom random) =>
        TimeSpan.FromMilliseconds(random.Next(CalculateBackoff(backoffBaseMs, capMs, ceiling, attempt) + 1));

    private static TimeSpan EqualJitter(int backoffBaseMs, int capMs, int ceiling, int attempt, IRandom random)
    {
        var calculatedBackoff = CalculateBackoff(backoffBaseMs, capMs, ceiling, attempt);
        var temp = calculatedBackoff / 2;

        return TimeSpan.FromMilliseconds(temp + calculatedBackoff % 2 + random.Next(temp + 1));
    }

    private static int CalculateBackoff(int backoffBaseMs, int capMs, int ceiling, int attempt) =>
        Math.Min(backoffBaseMs * (1 << Math.Min(ceiling, attempt)), capMs);
}
