using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

internal interface IYdbTypeMapping
{
    /// <summary>
    /// The database type used by YDB.
    /// </summary>
    YdbDbType YdbDbType { get; }
}
