using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

internal static class TypedValueFactory
{
    public static TypedValue FromObjects<T>(
        IReadOnlyCollection<T> rows,
        IReadOnlyList<string> columnNames,
        IReadOnlyList<Func<T, YdbValue>> columnSelectors)
    {
        if (rows.Count == 0)
            throw new ArgumentException("Rows collection is empty.", nameof(rows));
        if (columnNames.Count != columnSelectors.Count)
            throw new ArgumentException("Column names count must match selectors count.");

        var structs = new List<YdbValue>(rows.Count);

        foreach (var row in rows)
        {
            var members = new Dictionary<string, YdbValue>(columnNames.Count);
            for (int i = 0; i < columnNames.Count; i++)
            {
                var value = columnSelectors[i](row);
                members[columnNames[i]] = value;
            }

            structs.Add(YdbValue.MakeStruct(members));
        }

        var list = YdbValue.MakeList(structs);
        return list.GetProto();
    }
}