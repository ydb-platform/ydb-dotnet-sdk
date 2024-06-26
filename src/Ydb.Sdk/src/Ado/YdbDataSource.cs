#if NET7_0
using System.Data.Common;
# endif

using Ydb.Sdk.Services.Query.Pool;

namespace Ydb.Sdk.Ado;

public class YdbDataSource
#if NET7_0_OR_GREATER
    : DbDataSource
# endif
{
    private readonly SessionPool _sessionPool;

    internal YdbDataSource(SessionPool sessionPool, YdbConnectionStringBuilder connectionString)
    {
        _sessionPool = sessionPool;
        ConnectionString = connectionString.ConnectionString;
        Database = connectionString.Database;
    }
#if NET6_0
    public YdbConnection CreateConnection()
# endif
#if NET7_0_OR_GREATER
    protected override YdbConnection CreateDbConnection()
# endif
    {
        throw new NotImplementedException();
    }

    public
#if NET7_0_OR_GREATER
        override
# endif
        string ConnectionString { get; }

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
