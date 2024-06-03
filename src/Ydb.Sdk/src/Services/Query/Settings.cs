using Ydb.Query;

namespace Ydb.Sdk.Services.Query;

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

public class ExecuteQuerySettings : GrpcRequestSettings
{
    internal new static readonly ExecuteQuerySettings DefaultInstance = new();

    public Syntax Syntax { get; set; } = Syntax.YqlV1;
    public bool ConcurrentResultSets { get; set; }
}

public enum TxMode
{
    Unspecified,

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

    internal static TransactionControl? TransactionControl(this TxMode mode, bool autocommit = true)
    {
        return mode switch
        {
            TxMode.Unspecified => null,
            _ => new TransactionControl { BeginTx = mode.TransactionSettings(), CommitTx = autocommit }
        };
    }
}

public class ExecuteQueryPart
{
    public Status Status { get; }
    public Value.ResultSet? ResultSet { get; }
    public string TxId { get; }

    internal ExecuteQueryPart(Status status, Value.ResultSet? resultSet, string txId)
    {
        Status = status;
        ResultSet = resultSet;
        TxId = txId;
    }
}
