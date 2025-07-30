using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

public interface IBulkUpsertImporter : IAsyncDisposable
{
    ValueTask AddRowAsync(params YdbValue[] values);
    ValueTask AddRowAsync(params object?[] values);

    ValueTask AddRowsAsync(IEnumerable<YdbValue[]> rows, CancellationToken cancellationToken = default);
    ValueTask AddRowsAsync(IEnumerable<object?[]> rows, CancellationToken cancellationToken = default);

    ValueTask FlushAsync(CancellationToken cancellationToken = default);
    IReadOnlyList<Ydb.Value> GetBufferedRows();
}
