using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbDateTimeTypeMapping(
    string storeType,
    DbType? dbType
) : DateTimeTypeMapping(storeType, dbType)
{
    private const string DateTimeFormatConst = @"{0:yyyy-MM-dd HH\:mm\:ss.fffffff}";

    private string StoreTypeLiteral { get; } = storeType;

    protected override string SqlLiteralFormatString
        => "CAST('" + DateTimeFormatConst + $"' AS {StoreTypeLiteral})";
}
