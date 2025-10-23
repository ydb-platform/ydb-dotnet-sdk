using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado.RetryPolicy;

/// <summary>
/// Default retry policy implementation for YDB operations using exponential backoff with jitter.
/// </summary>
/// <remarks>
/// YdbRetryPolicy implements the recommended retry strategy for YDB operations based on
/// <a href="https://aws.amazon.com/ru/blogs/architecture/exponential-backoff-and-jitter/">AWS best practices</a>.
/// It uses different backoff strategies for different types of errors and includes jitter
/// to prevent thundering herd problems. This is the recommended implementation of <see cref="IRetryPolicy"/>.
/// </remarks>
public class YdbRetryPolicy : IRetryPolicy
{
    /// <summary>
    /// Gets the default retry policy instance with default configuration.
    /// </summary>
    /// <remarks>
    /// This instance uses the default configuration from <see cref="YdbRetryPolicyConfig.Default"/>.
    /// It's suitable for most use cases and provides a good balance between retry frequency and performance.
    /// </remarks>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbRetryPolicy"/> class with the specified configuration.
    /// </summary>
    /// <param name="config">The <see cref="YdbRetryPolicyConfig"/> retry policy configuration.</param>
    /// <remarks>
    /// This constructor creates a retry policy with the specified configuration.
    /// The policy will use different backoff strategies based on the error type and attempt number.
    /// </remarks>
    public YdbRetryPolicy(YdbRetryPolicyConfig config)
    {
        _maxAttempt = config.MaxAttempts;
        _fastBackoffBaseMs = config.FastBackoffBaseMs;
        _slowBackoffBaseMs = config.SlowBackoffBaseMs;
        _fastCeiling = (int)Math.Ceiling(Math.Log(config.FastCapBackoffMs + 1, 2));
        _slowCeiling = (int)Math.Ceiling(Math.Log(config.SlowCapBackoffMs + 1, 2));
        _fastCapBackoffMs = config.FastCapBackoffMs;
        _slowCapBackoffMs = config.SlowCapBackoffMs;
        _enableRetryIdempotence = config.EnableRetryIdempotence;
        _random = ThreadLocalRandom.Instance;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbRetryPolicy"/> class with the specified configuration and random number generator.
    /// </summary>
    /// <param name="config">The <see cref="YdbRetryPolicyConfig"/> retry policy configuration.</param>
    /// <param name="random">The random number generator for jitter calculations.</param>
    /// <remarks>
    /// This constructor is used for testing purposes to provide deterministic behavior.
    /// In production code, use the constructor that takes only the configuration parameter.
    /// </remarks>
    internal YdbRetryPolicy(YdbRetryPolicyConfig config, IRandom random) : this(config)
    {
        _random = random;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// <para>This method implements different retry strategies based on the YDB status code:</para>
    /// <para>- BadSession/SessionBusy: Immediate retry (TimeSpan.Zero)</para>
    /// <para>- Aborted/Undetermined: Fast backoff with full jitter</para>
    /// <para>- Unavailable/Transport errors: Fast backoff with equal jitter</para>
    /// <para>- Overloaded/Resource exhausted: Slow backoff with equal jitter</para>
    /// <para>- Other errors: No retry (null)</para>
    /// 
    /// <para>The policy respects the maximum attempt limit and idempotence settings.</para>
    /// </remarks>
    public TimeSpan? GetNextDelay(YdbException ydbException, int attempt)
    {
        if (attempt >= _maxAttempt - 1 || (!_enableRetryIdempotence && !ydbException.IsTransient))
            return null;

        return ydbException.Code switch
        {
            StatusCode.BadSession or StatusCode.SessionBusy or StatusCode.SessionExpired => TimeSpan.Zero,
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
