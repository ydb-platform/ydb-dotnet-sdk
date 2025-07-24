using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Table;

public static class BulkUpsertExtensions
{
    public static async Task BulkUpsertWithRetry<T>(
        this YdbConnection connection,
        string tablePath,
        IReadOnlyCollection<T> rows,
        IReadOnlyList<string> columns,
        IReadOnlyList<Func<T, YdbValue>> selectors,
        CancellationToken cancellationToken = default) =>
        await connection.BulkUpsertInternalAsync(
            tablePath,
            rows,
            columns,
            selectors,
            cancellationToken
        );
}