using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

internal class YdbTableIndex
{
    public YdbTableIndex(TableIndexDescription index)
    {
        Name = index.Name;
        DataColumns = index.DataColumns;
        IndexColumns = index.IndexColumns;
        Type = index.TypeCase switch
        {
            TableIndexDescription.TypeOneofCase.GlobalIndex => IndexType.GlobalIndex,
            TableIndexDescription.TypeOneofCase.GlobalAsyncIndex => IndexType.GlobalAsyncIndex,
            TableIndexDescription.TypeOneofCase.GlobalUniqueIndex => IndexType.GlobalUniqueIndex,
            _ => throw new YdbException($"Unexpected index type: {index.TypeCase}")
        };
    }
    
    public string Name { get; }
    
    public IndexType Type { get; }
    
    public IReadOnlyList<string> IndexColumns { get; }
    
    public IReadOnlyList<string> DataColumns { get; }
    
    public enum IndexType
    {
        GlobalIndex,
        GlobalAsyncIndex,
        GlobalUniqueIndex
    }
}
