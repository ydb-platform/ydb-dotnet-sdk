using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public sealed class YdbULongTypeMapping : ULongTypeMapping
{
    public YdbULongTypeMapping(string storeType, System.Data.DbType dbType) : base(storeType, dbType)
    {
    }

    private YdbULongTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override YdbULongTypeMapping Clone(RelationalTypeMappingParameters parameters) => new(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => base.GenerateNonNullSqlLiteral(value) + "ul";
}
