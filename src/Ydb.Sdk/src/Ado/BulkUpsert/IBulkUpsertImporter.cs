using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter : IAsyncDisposable
{
    ValueTask AddRowsAsync(IEnumerable<YdbValue[]> rows);
    ValueTask AddRowsAsync(IEnumerable<object?[]> rows);

    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
