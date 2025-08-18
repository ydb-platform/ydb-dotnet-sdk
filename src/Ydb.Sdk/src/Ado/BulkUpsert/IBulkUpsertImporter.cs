using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter
{
    /// <summary>Add a single row to the batch. Values must match the importer column order.</summary>
    /// <param name="row">Column values in the same order as the configured <c>columns</c>.</param>
    ValueTask AddRowAsync(params object[] row);

    /// <summary>
    /// Add many rows from <see cref="YdbList"/> (shape: <c>List&lt;Struct&lt;...&gt;&gt;</c>).
    /// Struct member names and order must exactly match the configured <c>columns</c>.
    /// </summary>
    ValueTask AddListAsync(YdbList list);

    /// <summary>Flush the current batch via BulkUpsert (no-op if empty).</summary>
    ValueTask FlushAsync();
}
