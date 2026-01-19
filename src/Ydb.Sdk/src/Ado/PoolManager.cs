using System.Collections.Concurrent;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado;

internal static class PoolManager
{
    private static readonly SemaphoreSlim SemaphoreSlim = new(1); // async mutex

    internal static readonly ConcurrentDictionary<string, IDriver> Drivers = new();
    internal static readonly ConcurrentDictionary<string, ISessionSource> Pools = new();

    internal static async ValueTask<ISessionSource> Get(
        YdbConnectionStringBuilder settings,
        CancellationToken cancellationToken
    )
    {
        if (Pools.TryGetValue(settings.ConnectionString, out var sessionPool))
        {
            return sessionPool;
        }

        await SemaphoreSlim.WaitAsync(cancellationToken);

        try
        {
            if (Pools.TryGetValue(settings.ConnectionString, out var pool))
            {
                return pool;
            }

            var driver = await GetDriver(settings, withLock: false);

            return Pools[settings.ConnectionString] = settings.EnableImplicitSession
                ? new ImplicitSessionSource(driver, settings.LoggerFactory)
                : new PoolingSessionSource<PoolingSession>(new PoolingSessionFactory(driver, settings), settings);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    internal static async ValueTask<IDriver> GetDriver(IDriverFactory driverFactory, bool withLock = true)
    {
        try
        {
            if (withLock)
                await SemaphoreSlim.WaitAsync();

            var driver = Drivers.TryGetValue(driverFactory.GrpcConnectionString, out var cacheDriver) &&
                         !cacheDriver.IsDisposed
                ? cacheDriver
                : Drivers[driverFactory.GrpcConnectionString] = await driverFactory.CreateAsync();
            driver.RegisterOwner();

            // ReSharper disable once InvertIf
            if (driver.IsDisposed) // detect race condition on open / close driver
            {
                driver = Drivers[driverFactory.GrpcConnectionString] = await driverFactory.CreateAsync();
                driver.RegisterOwner();
            }

            return driver;
        }
        finally
        {
            if (withLock)
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
