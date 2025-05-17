using System;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

// TODO: Await DateOnly support in Ydb.Sdk
public class YdbDateOnlyTypeMapping : RelationalTypeMapping
{
    private const string DateOnlyFormatConst = "{0:yyyy-MM-dd}";

    public YdbDateOnlyTypeMapping(string storeType)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateOnly)),
                storeType,
                StoreTypePostfix.None,
                System.Data.DbType.Date
            )
        )
    {
    }

    protected YdbDateOnlyTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbDateOnlyTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dateOnly = (DateOnly)value;
        return $"Date('{dateOnly.ToString(DateOnlyFormatConst)}')";
    }
}
