using Ydb.Table;

namespace Ydb.Sdk.Ado.Schema;

internal class YdbColumn
{
    internal YdbColumn(ColumnMeta columnMeta)
    {
        Name = columnMeta.Name;
        StorageType = columnMeta.Type.YqlTableType();
        IsNullable = columnMeta.Type.TypeCase == Type.TypeOneofCase.OptionalType;
        Family = columnMeta.Family;
    }

    public string Name { get; }

    public string StorageType { get; }

    public bool IsNullable { get; }

    public string Family { get; }
}
