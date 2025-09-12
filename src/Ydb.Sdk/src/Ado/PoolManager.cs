using System.Collections.Concurrent;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado;

internal static class PoolManager
{
    private static readonly SemaphoreSlim SemaphoreSlim = new(1); // async mutex

    internal static readonly ConcurrentDictionary<string, IDriver> Drivers = new();
    internal static readonly ConcurrentDictionary<string, ISessionSource> Pools = new();

    internal static async Task<ISession> GetSession(
        YdbConnectionStringBuilder settings,
        CancellationToken cancellationToken
    )
    {
        if (Pools.TryGetValue(settings.ConnectionString, out var sessionPool))
        {
            return await sessionPool.OpenSession(cancellationToken);
        }

        await SemaphoreSlim.WaitAsync(cancellationToken);

        try
        {
            if (Pools.TryGetValue(settings.ConnectionString, out var pool))
            {
                return await pool.OpenSession(cancellationToken);
            }

            var driver = Drivers.TryGetValue(settings.GrpcConnectionString, out var cacheDriver) &&
                         !cacheDriver.IsDisposed
                ? cacheDriver
                : Drivers[settings.GrpcConnectionString] = await settings.BuildDriver();

            ISessionSource newSessionPool;
            driver.RegisterOwner();
            if (settings.EnableImplicitSession)
            {
                newSessionPool = new ImplicitSessionSource(driver);
            }
            else
            {
                var factory = new PoolingSessionFactory(driver, settings);
                newSessionPool = new PoolingSessionSource<PoolingSession>(factory, settings);
            }

            Pools[settings.ConnectionString] = newSessionPool;

            return await newSessionPool.OpenSession(cancellationToken);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    internal static async Task ClearPool(string connectionString)
    {
        if (Pools.TryRemove(connectionString, out var sessionPool))
        {
            await SemaphoreSlim.WaitAsync();
            try
            {
                await sessionPool.DisposeAsync();
            }
            finally
            {
                SemaphoreSlim.Release();
            }
        }
    }

    internal static async Task ClearAllPools()
    {
        var keys = Pools.Keys.ToList();

        var tasks = keys.Select(ClearPool).ToList();

        await Task.WhenAll(tasks);
    }
}
