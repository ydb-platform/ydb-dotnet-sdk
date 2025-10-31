// ReSharper disable once CheckNamespace

namespace Ydb.Sdk.Ado;

/// <summary>
/// Specifies the transaction isolation mode for YDB operations.
/// </summary>
/// <remarks>
/// TransactionMode defines the isolation level and consistency guarantees
/// for database operations within a transaction.
///
/// <para>
/// For more information about YDB transaction modes, see:
/// <see href="https://ydb.tech/docs/en/concepts/transactions">YDB Transactions Documentation</see>.
/// </para>
/// </remarks>
public enum TransactionMode
{
    /// <summary>
    /// Serializable read-write transaction mode.
    /// </summary>
    /// <remarks>
    /// Provides the strictest isolation level for custom transactions.
    /// Guarantees that the result of successful parallel transactions is equivalent
    /// to their serial execution, with no read anomalies for successful transactions.
    /// This is the default mode for read-write operations.
    /// </remarks>
    SerializableRw,

    /// <summary>
    /// Snapshot read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// All read operations within the transaction access the database snapshot.
    /// All data reads are consistent. The snapshot is taken when the transaction begins,
    /// meaning the transaction sees all changes committed before it began.
    /// Only read operations are allowed.
    /// </remarks>
    SnapshotRo,

    /// <summary>
    /// Stale read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Read operations within the transaction may return results that are slightly
    /// out-of-date (lagging by fractions of a second). Each individual read returns
    /// consistent data, but no consistency between different reads is guaranteed.
    /// Only read operations are allowed.
    /// </remarks>
    StaleRo,

    /// <summary>
    /// Online read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Each read operation in the transaction reads the data that is most recent
    /// at execution time. Each individual read operation returns consistent data,
    /// but no consistency is guaranteed between reads. Reading the same table range
    /// twice may return different results.
    /// Only read operations are allowed.
    /// </remarks>
    OnlineRo,

    /// <summary>
    /// Online inconsistent read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Each read operation in the transaction reads the data that is most recent
    /// at execution time. Even the data fetched by a particular read operation may
    /// contain inconsistent results. This mode provides the highest performance
    /// but the lowest consistency guarantees.
    /// Only read operations are allowed.
    /// </remarks>
    OnlineInconsistentRo
}
