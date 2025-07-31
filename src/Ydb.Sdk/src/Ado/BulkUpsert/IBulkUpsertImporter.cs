namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter
{
    ValueTask AddRowAsync(object?[] row);

    ValueTask FlushAsync();
}
