namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter
{
    ValueTask AddRowAsync(object?[] rows);
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
