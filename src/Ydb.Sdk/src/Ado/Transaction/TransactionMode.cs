// ReSharper disable once CheckNamespace
namespace Ydb.Sdk.Ado;

public enum TransactionMode
{
    SerializableRw,
    SnapshotRo,
    StaleRo,

    OnlineRo,
    OnlineInconsistentRo
}
