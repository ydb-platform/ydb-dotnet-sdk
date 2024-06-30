#if NET7_0_OR_GREATER
using System.Data.Common;

using Ydb.Sdk.Services.Query.Pool;

namespace Ydb.Sdk.Ado;

public class YdbDataSource : DbDataSource
{
    private readonly SessionPool _sessionPool;

    internal YdbDataSource(SessionPool sessionPool, YdbConnectionStringBuilder connectionString)
    {
        _sessionPool = sessionPool;
        ConnectionString = connectionString.ConnectionString;
        Database = connectionString.Database;
    }

    protected override YdbConnection CreateDbConnection()
    {
        throw new NotImplementedException();
    }

    public override string ConnectionString { get; }

    internal string Database { get; }

    internal async Task<Session> GetSessionAsync()
    {
        var (status, session) = await _sessionPool.GetSession();

        if (status.IsSuccess)
        {
            return session!;
        }

        throw new YdbAdoException(status);
    }
}
# endif
