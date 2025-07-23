using Ydb.Operations;
using Ydb.Sdk.Ado.BulkUpsert;
using Ydb.Sdk.Client;
using Ydb.Table;

namespace Ydb.Sdk.Services.Table;

public static class BulkUpsertExtensions
{
    public static async Task<IResponse> BulkUpsertWithRetry<T>(
        this TableClient tableClient,
        string tablePath,
        IReadOnlyCollection<T> rows,
        RetrySettings? retrySettings = null) =>
        await tableClient.SessionExec(
            async session =>
            {
                var req = new BulkUpsertRequest
                {
                    Table = tablePath,
                    OperationParams = new OperationParams(),
                    Rows = TypedValueFactory.FromObjects(rows)
                };
                var resp = await session.BulkUpsertAsync(req);
                return new BulkUpsertResponseAdapter(resp);
            },
            retrySettings
        );
}