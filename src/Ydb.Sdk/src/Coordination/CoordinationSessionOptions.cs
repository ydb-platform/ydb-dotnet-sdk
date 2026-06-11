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
    /// Maximum time the client waits for the initial <c>SessionStarted</c> reply during attach.
    /// </summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Server-side session timeout. If the server does not see any client traffic within this
    /// window it expires the session. The server keeps the session alive via Ping/Pong driven
    /// by this value, so it should be comfortably larger than network round-trip times.
    /// </summary>
    public TimeSpan SessionTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Retry policy used by the session worker when reopening the underlying bidirectional stream.
    /// </summary>
    public IRetryPolicy RetryPolicy { get; init; } = YdbRetryPolicy.Default;
}
