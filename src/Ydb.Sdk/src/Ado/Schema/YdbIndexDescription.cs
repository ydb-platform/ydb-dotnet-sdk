using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

public sealed class YdbIndexDescription
{
    public string Name { get; }
    public YdbIndexType Type { get; }
    public IReadOnlyList<string> Columns { get; }

    internal YdbIndexDescription(TableIndexDescription index) : this(index.Name, index.TypeCase switch
    {
        TableIndexDescription.TypeOneofCase.GlobalIndex => YdbIndexType.Global,
        TableIndexDescription.TypeOneofCase.GlobalAsyncIndex => YdbIndexType.GlobalAsync,
        TableIndexDescription.TypeOneofCase.GlobalUniqueIndex => YdbIndexType.GlobalUnique,
        _ => throw new YdbException($"Unexpected index type: {index.TypeCase}")
    }, index.IndexColumns)
    {
        CoverColumns = index.DataColumns;
    }

    public YdbIndexDescription(string name, YdbIndexType type, IReadOnlyList<string> columns)
    {
        Name = name;
        Type = type;
        Columns = columns;
    }

    public IReadOnlyList<string> CoverColumns { get; init; } = Array.Empty<string>();

    internal TableIndex ToProto()
    {
        var tableIndex = new TableIndex { Name = Name };

        foreach (var column in Columns)
            tableIndex.IndexColumns.Add(column);
        foreach (var coverColumn in CoverColumns)
            tableIndex.DataColumns.Add(coverColumn);

        switch (Type)
        {
            case YdbIndexType.Global:
                tableIndex.GlobalIndex = new GlobalIndex();
                break;
            case YdbIndexType.GlobalAsync:
                tableIndex.GlobalAsyncIndex = new GlobalAsyncIndex();
                break;
            case YdbIndexType.GlobalUnique:
                tableIndex.GlobalUniqueIndex = new GlobalUniqueIndex();
                break;
            default:
                throw new YdbException($"Unexpected index type: {Type}");
        }

        return tableIndex;
    }
}

public enum YdbIndexType
{
    Global,
    GlobalAsync,
    GlobalUnique
}
