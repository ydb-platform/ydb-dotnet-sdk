namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter : IAsyncDisposable
{
    ValueTask AddRowAsync(params Ydb.Value[] values);
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
