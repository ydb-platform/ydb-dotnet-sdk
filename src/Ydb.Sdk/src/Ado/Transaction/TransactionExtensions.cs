using Ydb.Query;

namespace Ydb.Sdk.Ado.Transaction;

internal static class TransactionExtensions
{
    private static readonly TransactionSettings SerializableRw = new()
        { SerializableReadWrite = new SerializableModeSettings() };

    private static readonly TransactionSettings SnapshotRw = new()
        { SnapshotReadWrite = new SnapshotRWModeSettings() };

    private static readonly TransactionSettings SnapshotRo = new()
        { SnapshotReadOnly = new SnapshotModeSettings() };

    private static readonly TransactionSettings StaleRo = new() { StaleReadOnly = new StaleModeSettings() };

    private static readonly TransactionSettings OnlineRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = false } };

    private static readonly TransactionSettings OnlineInconsistentRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = true } };

    internal static TransactionSettings TransactionSettings(this TransactionMode mode) =>
        mode switch
        {
            TransactionMode.SerializableRw => SerializableRw,
            TransactionMode.SnapshotRw => SnapshotRw,
            TransactionMode.SnapshotRo => SnapshotRo,
            TransactionMode.StaleRo => StaleRo,
            TransactionMode.OnlineRo => OnlineRo,
            TransactionMode.OnlineInconsistentRo => OnlineInconsistentRo,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };

    internal static TransactionControl TransactionControl(this TransactionMode mode, bool commit = true) =>
        new() { BeginTx = mode.TransactionSettings(), CommitTx = commit };
}
