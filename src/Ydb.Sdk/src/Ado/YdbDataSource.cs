#if NET7_0_OR_GREATER
using System.Data.Common;

using Ydb.Sdk.Services.Query.Pool;

namespace Ydb.Sdk.Ado;

public class YdbDataSource : DbDataSource
{
    internal YdbDataSource(YdbConnectionStringBuilder connectionString)
    {
        ConnectionString = connectionString.ConnectionString;
        Database = connectionString.Database;
    }

    protected override YdbConnection CreateDbConnection()
    {
        throw new NotImplementedException();
    }

    public override string ConnectionString { get; }

    internal string Database { get; }
}
# endif
