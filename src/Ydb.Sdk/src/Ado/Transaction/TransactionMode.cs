// ReSharper disable once CheckNamespace

namespace Ydb.Sdk.Ado;

/// <summary>
/// Specifies the transaction isolation mode for YDB operations.
/// </summary>
/// <remarks>
/// TransactionMode defines the isolation level and consistency guarantees
/// for database operations within a transaction.
/// </remarks>
public enum TransactionMode
{
    /// <summary>
    /// Serializable read-write transaction mode.
    /// </summary>
    /// <remarks>
    /// Provides the highest isolation level with full ACID guarantees.
    /// All reads and writes are serializable, ensuring complete consistency.
    /// This is the default mode for read-write operations.
    /// </remarks>
    SerializableRw,

    /// <summary>
    /// Snapshot read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Provides a consistent snapshot of the database at a specific point in time.
    /// All reads within the transaction see the same consistent state.
    /// Only read operations are allowed.
    /// </remarks>
    SnapshotRo,

    /// <summary>
    /// Stale read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Allows reading potentially stale data for better performance.
    /// Provides eventual consistency guarantees but may return outdated information.
    /// Only read operations are allowed.
    /// </remarks>
    StaleRo,

    /// <summary>
    /// Online read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Provides real-time read access with strong consistency.
    /// Reads the most recent committed data without allowing inconsistent reads.
    /// Only read operations are allowed.
    /// </remarks>
    OnlineRo,

    /// <summary>
    /// Online inconsistent read-only transaction mode.
    /// </summary>
    /// <remarks>
    /// Provides real-time read access but allows inconsistent reads for better performance.
    /// May return data from different points in time within the same transaction.
    /// Only read operations are allowed.
    /// </remarks>
    OnlineInconsistentRo
}
