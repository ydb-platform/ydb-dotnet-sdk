using Ydb.Scheme;
using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

internal class YdbTable
{
    internal YdbTable(string name, DescribeTableResult describeTableResult)
    {
        Name = name;
        IsSystem = name.IsSystem();
        Type = describeTableResult.Self.Type switch
        {
            Entry.Types.Type.Table => TableType.Table,
            Entry.Types.Type.ColumnTable => TableType.ColumnTable,
            Entry.Types.Type.ExternalTable => TableType.ExternalTable,
            _ => throw new YdbException($"Unexpected schema object type: {describeTableResult.Self.Type}")
        };
        Columns = describeTableResult.Columns.Select(column => new YdbColumn(column)).ToList();
        PrimaryKey = describeTableResult.PrimaryKey;
        Indexes = describeTableResult.Indexes.Select(index => new YdbTableIndex(index)).ToList();
        YdbTableStats = describeTableResult.TableStats != null
            ? new YdbTableStats(describeTableResult.TableStats)
            : null;
    }

    public string Name { get; }

    public bool IsSystem { get; }

    public TableType Type { get; }

    public IReadOnlyList<YdbColumn> Columns { get; }

    public IReadOnlyList<string> PrimaryKey { get; }

    public IReadOnlyList<YdbTableIndex> Indexes { get; }
    
    public YdbTableStats? YdbTableStats { get; }

    public enum TableType
    {
        Table,
        ColumnTable,
        ExternalTable
    }
}
