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
        DbType? dbType
    ) : base(storeType, dbType)
    {
        StoreTypeLiteral = storeType;
    }

    protected override string SqlLiteralFormatString
        => "CAST('" + DateTimeFormatConst + $"' AS {StoreTypeLiteral})";
}
