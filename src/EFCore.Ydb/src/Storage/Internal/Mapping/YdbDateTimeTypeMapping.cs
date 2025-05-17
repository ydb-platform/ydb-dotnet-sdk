using System;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbDateTimeTypeMapping : DateTimeTypeMapping
{
    private const string DateTimeFormatConst = @"{0:yyyy-MM-dd HH\:mm\:ss.fffffff}";

    private string StoreTypeLiteral { get; }

    public YdbDateTimeTypeMapping(
        string storeType,
        DbType? dbType,
        Type clrType
    ) : base(storeType, dbType)
    {
        StoreTypeLiteral = storeType;
    }

    protected YdbDateTimeTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override string SqlLiteralFormatString
        => "CAST('" + DateTimeFormatConst + $"' AS {StoreTypeLiteral})";
}
