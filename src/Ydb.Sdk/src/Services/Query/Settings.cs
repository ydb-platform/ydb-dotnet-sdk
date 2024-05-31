using Ydb.Query;

namespace Ydb.Sdk.Services.Query;

public enum ExecMode
{
    Unspecified = 0,
    Parse = 10,
    Validate = 20,
    Explain = 30,

    // reserved 40; // EXEC_MODE_PREPARE
    Execute = 50
}

public enum Syntax
{
    Unspecified = 0,

    /// <summary>
    /// YQL
    /// </summary>
    YqlV1 = 1,

    /// <summary>
    /// PostgresSQL
    /// </summary>
    Pg = 2
}

public enum StatsMode
{
    Unspecified = 0,

    /// <summary>
    /// Stats collection is disabled
    /// </summary>
    None = 10,

    /// <summary>
    /// Aggregated stats of reads, updates and deletes per table
    /// </summary>
    Basic = 20,

    /// <summary>
    /// Add execution stats and plan on top of STATS_MODE_BASIC
    /// </summary>
    Full = 30,

    /// <summary>
    /// Detailed execution stats including stats for individual tasks and channels
    /// </summary>
    Profile = 40
}

public class ExecuteQuerySettings : GrpcRequestSettings
{
    public ExecMode ExecMode { get; set; } = ExecMode.Execute;
    public Syntax Syntax { get; set; } = Syntax.YqlV1;
    public StatsMode StatsMode { get; set; }
    public bool AutoCommit { get; set; } = true;
    public bool ConcurrentResultSets { get; set; }
    public string? TxId { get; set; }
}

public enum TxMode
{
    None,

    SerializableRw,
    SnapshotRo,
    StaleRo,

    OnlineRo,
    OnlineInconsistentRo
}

internal static class TxModeExtensions
{
    private static readonly TransactionSettings SerializableRw = new()
        { SerializableReadWrite = new SerializableModeSettings() };

    private static readonly TransactionSettings SnapshotRo = new()
        { SnapshotReadOnly = new SnapshotModeSettings() };

    private static readonly TransactionSettings StaleRo = new()
        { StaleReadOnly = new StaleModeSettings() };

    private static readonly TransactionSettings OnlineRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = false } };

    private static readonly TransactionSettings OnlineInconsistentRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = true } };

    internal static TransactionSettings? TransactionSettings(this TxMode mode)
    {
        return mode switch
        {
            TxMode.SerializableRw => SerializableRw,
            TxMode.SnapshotRo => SnapshotRo,
            TxMode.StaleRo => StaleRo,
            TxMode.OnlineRo => OnlineRo,
            TxMode.OnlineInconsistentRo => OnlineInconsistentRo,
            _ => null
        };
    }
}
