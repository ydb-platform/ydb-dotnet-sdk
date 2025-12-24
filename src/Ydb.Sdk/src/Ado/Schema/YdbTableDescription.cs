using Ydb.Scheme;
using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

/// <summary>
/// Describes the structure and properties of a YDB table.
/// </summary>
/// <remarks>
/// This class represents table metadata including columns, primary key, indexes, and optional statistics.
/// It is used both for describing existing tables (returned by <see cref="YdbDataSource.DescribeTable"/>)
/// and for creating new tables (passed to <see cref="YdbDataSource.CreateTable"/>).
/// </remarks>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbTableDescription"/> class for creating a new table.
    /// </summary>
    /// <param name="name">The name or path of the table.</param>
    /// <param name="columns">The list of column definitions for the table.</param>
    /// <param name="primaryKey">The list of column names that form the primary key.</param>
    /// <remarks>
    /// Use this constructor when creating a new table. The table type defaults to <see cref="YdbTableType.Raw"/>.
    /// You can set additional properties like <see cref="Type"/> and <see cref="Indexes"/> using object initializer syntax.
    /// </remarks>
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

    /// <summary>
    /// Gets the name or path of the table.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets a value indicating whether this is a system table.
    /// </summary>
    /// <value><c>true</c> if this is a system table; otherwise, <c>false</c>.</value>
    public bool IsSystem { get; }

    /// <summary>
    /// Gets or sets the table storage type.
    /// </summary>
    /// <value>The table type. Default is <see cref="YdbTableType.Raw"/>.</value>
    /// <remarks>
    /// This property determines how data is stored:
    /// - <see cref="YdbTableType.Raw"/>: Row-oriented storage (default)
    /// - <see cref="YdbTableType.Column"/>: Column-oriented storage
    /// - <see cref="YdbTableType.External"/>: External table (not supported via Control Plane RPC)
    /// </remarks>
    public YdbTableType Type { get; init; } = YdbTableType.Raw;

    /// <summary>
    /// Gets the list of column definitions for the table.
    /// </summary>
    public IReadOnlyList<YdbColumnDescription> Columns { get; }

    /// <summary>
    /// Gets the list of column names that form the primary key.
    /// </summary>
    public IReadOnlyList<string> PrimaryKey { get; }

    /// <summary>
    /// Gets or sets the list of index definitions for the table.
    /// </summary>
    /// <value>The list of indexes. Default is an empty list.</value>
    /// <remarks>
    /// Indexes can be added when creating a table using object initializer syntax.
    /// </remarks>
    public IReadOnlyList<YdbIndexDescription> Indexes { get; init; } = new List<YdbIndexDescription>();

    /// <summary>
    /// Gets the table statistics, if available.
    /// </summary>
    /// <value>
    /// The table statistics including row count estimates and storage size, or <c>null</c> if statistics were not requested.
    /// </value>
    /// <remarks>
    /// This property is populated when describing a table with <see cref="DescribeTableSettings.IncludeTableStats"/> set to <c>true</c>.
    /// </remarks>
    public YdbTableStats? TableStats { get; }
}

/// <summary>
/// Specifies the storage type for a YDB table.
/// </summary>
public enum YdbTableType
{
    /// <summary>
    /// Row-oriented storage (default). Data is stored row by row.
    /// </summary>
    Raw,

    /// <summary>
    /// Column-oriented storage. Data is stored column by column for better analytical query performance.
    /// </summary>
    Column,

    /// <summary>
    /// External table. References data stored outside YDB.
    /// Note: Not supported via Control Plane RPC operations.
    /// </summary>
    External
}
