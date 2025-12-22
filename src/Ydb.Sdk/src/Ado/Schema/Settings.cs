namespace Ydb.Sdk.Ado.Schema;

public readonly struct DescribeTableSettings
{
    /// <summary>
    /// Includes table statistics
    /// </summary>
    public bool IncludeTableStats { get; init; }
}

public readonly record struct CopyTableSettings(string SourceTable, string DestinationTable, bool OmitIndexes = false);

public readonly record struct RenameTableSettings(
    string SourceTable,
    string DestinationTable,
    bool ReplaceDestination = false
);
