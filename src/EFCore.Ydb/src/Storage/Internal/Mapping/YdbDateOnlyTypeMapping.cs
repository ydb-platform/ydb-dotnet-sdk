using System;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

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

    protected override YdbDateOnlyTypeMapping Clone(RelationalTypeMappingParameters parameters) => new(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dateOnly = (DateOnly)value;
        return $"Date('{dateOnly.ToString(DateOnlyFormatConst)}')";
    }
}
