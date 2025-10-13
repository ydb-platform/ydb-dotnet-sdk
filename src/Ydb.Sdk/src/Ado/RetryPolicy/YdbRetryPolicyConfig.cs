namespace Ydb.Sdk.Ado.RetryPolicy;

/// <summary>
/// Configuration settings for the <see cref="YdbRetryPolicy"/>.
/// </summary>
/// <remarks>
/// YdbRetryPolicyConfig provides configuration options for customizing the retry behavior
/// of YDB operations. The default values are suitable for most use cases, but can be
/// adjusted based on specific requirements and performance characteristics.
/// </remarks>
public class YdbRetryPolicyConfig
{
    /// <summary>
    /// Gets the default retry policy configuration.
    /// </summary>
    /// <remarks>
    /// This configuration provides a good balance between retry frequency and performance
    /// for most YDB operations. It can be used as a starting point for custom configurations.
    /// </remarks>
    public static readonly YdbRetryPolicyConfig Default = new();

    /// <summary>
    /// Gets or sets the maximum number of retry attempts.
    /// </summary>
    /// <remarks>
    /// The total number of attempts will be MaxAttempts (including the initial attempt).
    /// Setting this to 1 disables retries entirely.
    /// <para>Default value: 10.</para>
    /// </remarks>
    public int MaxAttempts { get; init; } = 10;

    /// <summary>
    /// Gets or sets the base delay in milliseconds for fast backoff strategies.
    /// </summary>
    /// <remarks>
    /// This is used for errors that typically resolve quickly, such as temporary
    /// unavailability or TLI (Transaction Lock Invalidated).
    /// The actual delay will be calculated using exponential backoff with jitter.
    /// <para>Default value: 5 ms.</para>
    /// </remarks>
    public int FastBackoffBaseMs { get; init; } = 5;

    /// <summary>
    /// Gets or sets the base delay in milliseconds for slow backoff strategies.
    /// </summary>
    /// <remarks>
    /// This is used for errors that may take longer to resolve, such as overload
    /// or resource exhaustion. The actual delay will be calculated using
    /// exponential backoff with jitter.
    /// <para>Default value: 50 ms.</para>
    /// </remarks>
    public int SlowBackoffBaseMs { get; init; } = 50;

    /// <summary>
    /// Gets or sets the maximum delay in milliseconds for fast backoff strategies.
    /// </summary>
    /// <remarks>
    /// This caps the maximum delay for fast backoff to prevent excessively long waits.
    /// The exponential backoff will not exceed this value.
    /// <para>Default value: 500 ms.</para>
    /// </remarks>
    public int FastCapBackoffMs { get; init; } = 500;

    /// <summary>
    /// Gets or sets the maximum delay in milliseconds for slow backoff strategies.
    /// </summary>
    /// <remarks>
    /// This caps the maximum delay for slow backoff to prevent excessively long waits.
    /// The exponential backoff will not exceed this value.
    /// <para>Default value: 5000 ms.</para>
    /// </remarks>
    public int SlowCapBackoffMs { get; init; } = 5_000;

    /// <summary>
    /// Gets or sets a value indicating whether to enable retry for idempotent statuses.
    /// </summary>
    /// <remarks>
    /// When false, only transient errors are retried. When true, all retryable statuses
    /// are retried, which means the operation may be executed twice. This happens because
    /// some statuses (like unavailable) don't indicate whether the server processed the
    /// operation - the connection might have been lost during the response. Enable this
    /// only if you are certain that the operations being retried are idempotent.
    /// <para>Default value: false.</para>
    /// </remarks>
    public bool EnableRetryIdempotence { get; init; } = false;

    /// <summary>
    /// Returns a string representation of the retry policy configuration.
    /// </summary>
    /// <returns>A string containing all configuration values in a readable format.</returns>
    /// <remarks>
    /// This method is useful for logging and debugging purposes to see the current
    /// retry policy configuration values.
    /// </remarks>
    public override string ToString() => $"MaxAttempt={MaxAttempts};" +
                                         $"FastBackoffBaseMs={FastBackoffBaseMs};" +
                                         $"SlowBackoffBaseMs={SlowBackoffBaseMs};" +
                                         $"FastCapBackoffMs={FastCapBackoffMs};" +
                                         $"SlowCapBackoffMs={SlowCapBackoffMs};" +
                                         $"EnableRetryIdempotence={EnableRetryIdempotence}";
}
