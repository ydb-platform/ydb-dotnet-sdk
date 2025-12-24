using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

/// <summary>
/// Describes an index on a YDB table.
/// </summary>
/// <remarks>
/// This class represents index metadata including name, type, indexed columns, and optional covering columns.
/// It is used both for describing existing table indexes and for defining indexes when creating new tables.
/// </remarks>
public sealed class YdbIndexDescription
{
    /// <summary>
    /// Gets the name of the index.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the type of the index.
    /// </summary>
    public YdbIndexType Type { get; }

    /// <summary>
    /// Gets the list of column names that are indexed.
    /// </summary>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbIndexDescription"/> class with the specified name, type, and columns.
    /// </summary>
    /// <param name="name">The name of the index.</param>
    /// <param name="type">The type of the index (Global, GlobalAsync, or GlobalUnique).</param>
    /// <param name="columns">The list of column names that are indexed.</param>
    public YdbIndexDescription(string name, YdbIndexType type, IReadOnlyList<string> columns)
    {
        Name = name;
        Type = type;
        Columns = columns;
    }

    /// <summary>
    /// Gets or sets the list of covering columns for this index.
    /// </summary>
    /// <value>
    /// The list of column names that are included in the index for covering queries.
    /// Default is an empty list.
    /// </value>
    /// <remarks>
    /// Covering columns allow the index to satisfy queries without accessing the main table.
    /// </remarks>
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

/// <summary>
/// Specifies the type of index on a YDB table.
/// </summary>
public enum YdbIndexType
{
    /// <summary>
    /// Global index. Provides fast lookups across partitions.
    /// </summary>
    Global,

    /// <summary>
    /// Global asynchronous index. Built asynchronously in the background.
    /// </summary>
    GlobalAsync,

    /// <summary>
    /// Global unique index. Enforces uniqueness across partitions.
    /// </summary>
    GlobalUnique
}
