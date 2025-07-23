namespace Ydb.Sdk.Ado.BulkUpsert;

public sealed record BulkUpsertOptions(
    TimeSpan? Timeout = null,
    CancellationToken CancellationToken = default
);