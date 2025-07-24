using System.Reflection;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Services.Table;

public static class BulkUpsertExtensions
{
    public static async Task BulkUpsertWithRetry<T>(
        this YdbConnection connection,
        string tablePath,
        IReadOnlyCollection<T> rows,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken = default)
    {
        var type = typeof(T);
        var props = columns
            .Select(col => type.GetProperty(col) ??
                           throw new ArgumentException($"Type {typeof(T).Name} does not have a property '{col}'"))
            .ToArray();

        var selectors = props
            .Select<PropertyInfo, Func<T, object?>>(p => x => p.GetValue(x))
            .ToArray();

        await connection.BulkUpsertInternalAsync(
            tablePath,
            rows,
            columns,
            selectors,
            cancellationToken
        );
    }
}