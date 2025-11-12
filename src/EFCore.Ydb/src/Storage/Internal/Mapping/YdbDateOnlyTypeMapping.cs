using System;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbDateOnlyTypeMapping : YdbTypeMapping
{
    public YdbDateOnlyTypeMapping(YdbDbType ydbDbType) : base(typeof(DateOnly), ydbDbType)
    {
    }

    private YdbDateOnlyTypeMapping(RelationalTypeMappingParameters parameters, YdbDbType ydbDbType)
        : base(parameters, ydbDbType)
    {
    }

    protected override YdbDateOnlyTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new(parameters, YdbDbType);

    protected override string SqlLiteralFormatString => $"{YdbDbType}('{{0:yyyy-MM-dd}}')";
}
