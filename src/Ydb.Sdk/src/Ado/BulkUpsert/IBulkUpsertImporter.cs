namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter
{
    ValueTask AddRowsAsync(IEnumerable<object?[]> rows);
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}