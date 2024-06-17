using Ydb.Sdk.Services.Query.Pool;

namespace Ydb.Sdk.Ado;

internal class YdbContext
{
    private readonly SessionPool _sessionPool;

    public string ConnectionString { get; }
    public string Database { get; }

    public YdbContext(SessionPool sessionPool, string connectionString, string database)
    {
        _sessionPool = sessionPool;
        ConnectionString = connectionString;
        Database = database;
    }

    public async Task<Session> GetSessionAsync()
    {
        var (status, session) = await _sessionPool.GetSession();

        if (status.IsSuccess)
        {
            return session!;
        }

        throw new YdbAdoException(status);
    }
};
