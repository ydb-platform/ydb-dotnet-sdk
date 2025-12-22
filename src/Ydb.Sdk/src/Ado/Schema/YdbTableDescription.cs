using Ydb.Scheme;
using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

public sealed class YdbTableDescription
{
    internal YdbTableDescription(string name, DescribeTableResult describeTableResult)
    {
        Name = name;
        IsSystem = name.IsSystem();
        Type = describeTableResult.Self.Type switch
        {
            Entry.Types.Type.Table => YdbTableType.Raw,
            Entry.Types.Type.ColumnTable => YdbTableType.Column,
            Entry.Types.Type.ExternalTable => YdbTableType.External,
            _ => throw new YdbException($"Unexpected schema object type: {describeTableResult.Self.Type}")
        };
        Columns = describeTableResult.Columns.Select(column => new YdbColumnDescription(column)).ToList();
        PrimaryKey = describeTableResult.PrimaryKey;
        Indexes = describeTableResult.Indexes.Select(index => new YdbIndexDescription(index)).ToList();
        TableStats = describeTableResult.TableStats != null
            ? new YdbTableStats(describeTableResult.TableStats)
            : null;
    }

    public YdbTableDescription(
        string name,
        IReadOnlyList<YdbColumnDescription> columns,
        IReadOnlyList<string> primaryKey
    )
    {
        Name = name;
        IsSystem = false;
        Columns = columns;
        PrimaryKey = primaryKey;
    }

    public string Name { get; }

    public bool IsSystem { get; }

    public YdbTableType Type { get; init; } = YdbTableType.Raw;

    public IReadOnlyList<YdbColumnDescription> Columns { get; }

    public IReadOnlyList<string> PrimaryKey { get; }

    public IReadOnlyList<YdbIndexDescription> Indexes { get; init; } = new List<YdbIndexDescription>();

    public YdbTableStats? TableStats { get; }
}

public enum YdbTableType
{
    Raw,
    Column,
    External
}
