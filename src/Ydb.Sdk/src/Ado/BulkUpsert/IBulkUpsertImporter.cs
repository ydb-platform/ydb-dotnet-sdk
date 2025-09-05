using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

/// <summary>
/// Bulk upsert importer API: add rows and flush them to YDB in batches.
/// </summary>
public interface IBulkUpsertImporter
{
    /// <summary>
    /// Add a single row to the batch. Values must match the importer’s column order.
    /// </summary>
    /// <param name="row">Values in the same order as the configured <c>columns</c>.</param>
    /// <exception cref="ArgumentException">Thrown when the number of values differs from the number of columns.</exception>
    ValueTask AddRowAsync(params object[] row);

    /// <summary>
    /// Add multiple rows from a <see cref="YdbList"/> shaped as <c>List&lt;Struct&lt;...&gt;&gt;</c>.
    /// Struct member names and order must exactly match the configured <c>columns</c>.
    /// </summary>
    /// <param name="list">Rows as <c>List&lt;Struct&lt;...&gt;&gt;</c> with the exact column names and order.</param>
    /// <exception cref="ArgumentException">
    /// Thrown when the struct column set, order, or count does not match the importer’s <c>columns</c>.
    /// </exception>
    ValueTask AddListAsync(YdbList list);

    /// <summary>
    /// Flush the current batch via BulkUpsert. No-op if the batch is empty.
    /// </summary>
    ValueTask FlushAsync();
}
