namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter
{
    ValueTask AddRowAsync(params object[] row);

    ValueTask FlushAsync();
}
