namespace Ydb.Sdk.Ado.Schema;

/// <summary>
/// Settings for the <see cref="YdbDataSource.DescribeTable"/> operation.
/// </summary>
/// <remarks>
/// These settings control what additional information is included in the table description response.
/// </remarks>
public readonly struct DescribeTableSettings
{
    /// <summary>
    /// Gets or sets a value indicating whether to include table statistics in the response.
    /// </summary>
    /// <value>
    /// <c>true</c> to include table statistics (row count estimates, storage size, etc.); otherwise, <c>false</c>.
    /// Default is <c>false</c>.
    /// </value>
    /// <remarks>
    /// When enabled, the response will include <see cref="YdbTableDescription.TableStats"/> with information
    /// such as estimated row count, storage size, and partition statistics.
    /// </remarks>
    public bool IncludeTableStats { get; init; }
}

/// <summary>
/// Settings for copying a table in the <see cref="YdbDataSource.CopyTables"/> operation.
/// </summary>
/// <param name="SourceTable">The name or path of the source table to copy.</param>
/// <param name="DestinationTable">The name or path of the destination table where the copy will be created.</param>
/// <param name="OmitIndexes">Whether to skip copying indexes. Default is <c>false</c> (indexes are copied).</param>
/// <remarks>
/// This record struct is used to specify source and destination tables for copy operations.
/// The table names can be simple names (resolved relative to the database path) or full paths.
/// </remarks>
public readonly record struct CopyTableSettings(string SourceTable, string DestinationTable, bool OmitIndexes = false);

/// <summary>
/// Settings for renaming a table in the <see cref="YdbDataSource.RenameTables"/> operation.
/// </summary>
/// <param name="SourceTable">The current name or path of the table to rename.</param>
/// <param name="DestinationTable">The new name or path for the table.</param>
/// <param name="ReplaceDestination">Whether to replace the destination table if it already exists. Default is <c>false</c>.</param>
/// <remarks>
/// This record struct is used to specify source and destination names for rename operations.
/// The table names can be simple names (resolved relative to the database path) or full paths.
/// 
/// <para>
/// If <paramref name="ReplaceDestination"/> is <c>false</c> and the destination table already exists,
/// the operation will fail with an error.
/// </para>
/// </remarks>
public readonly record struct RenameTableSettings(
    string SourceTable,
    string DestinationTable,
    bool ReplaceDestination = false
);
