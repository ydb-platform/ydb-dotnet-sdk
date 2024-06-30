using System.Collections.Concurrent;
using Ydb.Sdk.Services.Query.Pool;

namespace Ydb.Sdk.Ado;

public class PoolManager
{
    private static readonly SemaphoreSlim SemaphoreSlim = new(1); // async mutex

    private static ConcurrentDictionary<string, SessionPool> Pools { get; } = new();

    internal static async Task<Session> GetSession(YdbConnectionStringBuilder connectionString)
    {
        if (Pools.TryGetValue(connectionString.ConnectionString, out var sessionPool))
        {
            return await sessionPool.Session();
        }

        try
        {
            await SemaphoreSlim.WaitAsync();

            if (Pools.TryGetValue(connectionString.ConnectionString, out var pool))
            {
                return await pool.Session();
            }

            var newSessionPool = new SessionPool(await connectionString.BuildDriver(), connectionString.MaxSessionPool);

            Pools[connectionString.ConnectionString] = newSessionPool;

            return await newSessionPool.Session();
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }
}

internal static class SessionPoolExtension
{
    // TODO Retry policy
    internal static async Task<Session> Session(this SessionPool sessionPool)
    {
        var (status, session) = await sessionPool.GetSession();

        if (status.IsSuccess)
        {
            return session!;
        }

        throw new YdbAdoException(status);
    }
}
