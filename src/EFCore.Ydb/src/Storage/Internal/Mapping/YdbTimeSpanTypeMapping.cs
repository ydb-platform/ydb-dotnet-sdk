using System;
using System.Xml;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbTimeSpanTypeMapping : YdbTypeMapping
{
    public YdbTimeSpanTypeMapping(YdbDbType ydbDbType) : base(typeof(TimeSpan), ydbDbType)
    {
    }

    private YdbTimeSpanTypeMapping(RelationalTypeMappingParameters parameters, YdbDbType ydbDbType)
        : base(parameters, ydbDbType)
    {
    }

    protected override YdbTimeSpanTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new(parameters, YdbDbType);

    protected override string GenerateNonNullSqlLiteral(object value) =>
        $"{YdbDbType}('{XmlConvert.ToString((TimeSpan)value)}')";
}
