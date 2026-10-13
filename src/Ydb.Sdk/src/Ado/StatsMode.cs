namespace Ydb.Sdk.Ado;

/// <summary>Specifies the level of server statistics collected for a command.</summary>
/// <remarks>
/// Set <see cref="YdbCommand.StatsMode"/> before execution. Statistics are available through
/// <see cref="YdbCommand.QueryStats"/> only after the response stream has been fully consumed
/// successfully. The server may omit statistics or individual fields for any collection level.
/// </remarks>
public enum StatsMode
{
    /// <summary>Disables statistics collection.</summary>
    /// <remarks>This is the default for new commands.</remarks>
    None,

    /// <summary>Collects aggregated statistics for reads, updates and deletes.</summary>
    /// <remarks>Includes query timings and table operation counters.</remarks>
    Basic,

    /// <summary>Collects execution statistics and the query plan.</summary>
    /// <remarks>Includes the Basic statistics and a JSON query plan.</remarks>
    Full,

    /// <summary>Collects detailed execution statistics for tasks and channels.</summary>
    /// <remarks>Includes the Full statistics and per-task and per-channel details in the query plan.</remarks>
    Profile,

    /// <summary>Uses the server's default statistics collection mode.</summary>
    /// <remarks>The server selects the effective collection level instead of the client disabling it.</remarks>
    Unspecified
}
