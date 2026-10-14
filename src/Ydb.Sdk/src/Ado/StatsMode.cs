namespace Ydb.Sdk.Ado;

/// <summary>
/// Specifies the level of server statistics collected for a command.
/// </summary>
/// <remarks>
/// Set <see cref="YdbCommand.StatsMode"/> before execution. Statistics are available through
/// <see cref="YdbCommand.QueryStats"/> only after the response stream has been fully consumed
/// successfully. The server may omit statistics or individual fields for any collection level.
/// </remarks>
public enum StatsMode
{
    /// <summary>
    /// Disables statistics collection.
    /// </summary>
    /// <remarks>
    /// This is the default for new commands.
    /// </remarks>
    None = 10,

    /// <summary>
    /// Collects aggregated statistics for reads, updates and deletes.
    /// </summary>
    /// <remarks>
    /// Includes query timings and table operation counters.
    /// </remarks>
    Basic = 20,

    /// <summary>
    /// Collects execution statistics and the query plan.
    /// </summary>
    /// <remarks>
    /// Includes the Basic statistics and a JSON query plan.
    /// </remarks>
    Full = 30,

    /// <summary>
    /// Collects detailed execution statistics for tasks and channels.
    /// </summary>
    /// <remarks>
    /// Includes the Full statistics and per-task and per-channel details in the query plan.
    /// </remarks>
    Profile = 40,

    /// <summary>
    /// Uses the server's default statistics collection mode.
    /// </summary>
    /// <remarks>
    /// The server selects the effective collection level instead of the client disabling it.
    /// </remarks>
    Unspecified = 0
}

internal static class StatsModeExtensions
{
    internal static Query.StatsMode ToProto(this StatsMode mode) =>
        Enum.IsDefined(typeof(StatsMode), (int)mode)
            ? (Query.StatsMode)mode
            : throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
}
