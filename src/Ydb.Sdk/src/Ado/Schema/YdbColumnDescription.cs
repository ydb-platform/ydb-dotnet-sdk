using Ydb.Sdk.Ado.YdbType;
using Ydb.Table;
using static Ydb.Sdk.Ado.Internal.YdbTypeExtensions;

namespace Ydb.Sdk.Ado.Schema;

public sealed class YdbColumnDescription
{
    public string Name { get; }
    public YdbColumnType StorageType { get; }

    internal YdbColumnDescription(ColumnMeta columnMeta) : this(columnMeta.Name, new YdbColumnType(columnMeta.Type))
    {
        IsNullable = columnMeta.Type.TypeCase == Type.TypeOneofCase.OptionalType;
        Family = columnMeta.Family;
    }

    public YdbColumnDescription(string name, YdbColumnType ydbColumnType)
    {
        Name = name;
        StorageType = ydbColumnType;
    }

    public YdbColumnDescription(string name, YdbDbType ydbDbType) : this(name, new YdbColumnType(ydbDbType))
    {
    }

    public bool IsNullable { get; init; } = true;
    public string? Family { get; init; }

    internal ColumnMeta ToProto()
    {
        var columnMeta = new ColumnMeta
        {
            Name = Name,
            Type = StorageType.ToProto(),
            NotNull = !IsNullable,
        };

        if (IsNullable)
        {
            columnMeta.Type = columnMeta.Type.OptionalType();
        }

        if (Family != null)
        {
            columnMeta.Family = Family;
        }

        return columnMeta;
    }

}
