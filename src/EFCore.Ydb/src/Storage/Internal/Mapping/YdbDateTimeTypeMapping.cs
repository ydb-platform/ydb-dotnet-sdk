using System;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbDateTimeTypeMapping : YdbTypeMapping
{
    public YdbDateTimeTypeMapping(YdbDbType ydbDbType) : base(typeof(DateTime), ydbDbType)
    {
    }

    private YdbDateTimeTypeMapping(RelationalTypeMappingParameters parameters, YdbDbType ydbDbType)
        : base(parameters, ydbDbType)
    {
    }

    protected override YdbDateTimeTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new(parameters, YdbDbType);

    protected override string SqlLiteralFormatString => YdbDbType switch
    {
        YdbDbType.Timestamp or YdbDbType.Timestamp64 => $@"{YdbDbType}('{{0:yyyy-MM-ddTHH\:mm\:ss.ffffffZ}}')",
        YdbDbType.Datetime or YdbDbType.Datetime64 => $@"{YdbDbType}('{{0:yyyy-MM-ddTHH\:mm\:ssZ}}')",
        YdbDbType.Date or YdbDbType.Date32 => $"{YdbDbType}('{{0:yyyy-MM-dd}}')",
        _ => throw new ArgumentOutOfRangeException(nameof(YdbDbType), YdbDbType, null)
    };
}
