using Ydb.Sdk.Ado.RetryPolicy;

namespace Ydb.Sdk.Coordination;

/// <summary>
/// Configuration for a <see cref="CoordinationSession"/>.
/// </summary>
public sealed record CoordinationSessionOptions
{
    public static CoordinationSessionOptions Default { get; } = new();

    /// <summary>
    /// Arbitrary client-supplied description attached to the session on the server side.
    /// Useful for debugging — shows up in semaphore owner/waiter dumps.
    /// </summary>
    public string Description { get; init; } = "";

    /// <summary>
    /// Maximum time to wait for the initial <c>SessionStarted</c> reply from the server.
    /// </summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Retry policy used by the session worker when reopening the underlying bidirectional stream.
    /// </summary>
    public IRetryPolicy RetryPolicy { get; init; } = YdbRetryPolicy.Default;
}
