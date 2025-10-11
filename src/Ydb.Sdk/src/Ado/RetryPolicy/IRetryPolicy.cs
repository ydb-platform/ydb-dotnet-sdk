namespace Ydb.Sdk.Ado.RetryPolicy;

/// <summary>
/// Defines the contract for retry policies used by YDB operations.
/// </summary>
/// <remarks>
/// IRetryPolicy provides a way to implement custom retry logic for YDB operations.
/// The recommended implementation is <see cref="YdbRetryPolicy"/>, but custom implementations
/// can be created for specific use cases. When implementing a custom retry policy, ensure
/// you understand the implications of retrying operations and handle idempotency correctly.
/// </remarks>
public interface IRetryPolicy
{
    /// <summary>
    /// Calculates the delay before the next retry attempt.
    /// </summary>
    /// <param name="ydbException">The <see cref="YdbException"/> that was thrown during the last execution attempt.</param>
    /// <param name="attempt">The current attempt number (0-based).</param>
    /// <returns>
    /// The delay before the next retry attempt, or null if no more retries should be attempted.
    /// </returns>
    /// <remarks>
    /// This method is called for each retry attempt. Return null to stop retrying.
    /// Consider the <see cref="YdbException"/>, attempt number, and operation idempotency when making retry decisions.
    /// </remarks>
    public TimeSpan? GetNextDelay(YdbException ydbException, int attempt);
}
