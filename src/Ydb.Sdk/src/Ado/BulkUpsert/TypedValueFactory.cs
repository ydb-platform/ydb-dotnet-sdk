using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.BulkUpsert;

internal static class TypedValueFactory
{
    public static TypedValue FromObjects<T>(
        IReadOnlyCollection<T> rows,
        IReadOnlyList<string> columnNames,
        IReadOnlyList<Func<T, object?>> columnSelectors)
    {
        var structs = new List<YdbValue>(rows.Count);

        foreach (var row in rows)
        {
            var members = new Dictionary<string, YdbValue>(columnNames.Count);
            for (var i = 0; i < columnNames.Count; i++)
            {
                var val = columnSelectors[i](row);
                members[columnNames[i]] = new YdbParameter { Value = val }.YdbValue;
            }

            structs.Add(YdbValue.MakeStruct(members));
        }

        var list = YdbValue.MakeList(structs);
        return list.GetProto();
    }
}