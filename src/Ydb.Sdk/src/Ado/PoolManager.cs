using System.Collections.Concurrent;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado;

internal static class PoolManager
{
    private static readonly SemaphoreSlim SemaphoreSlim = new(1); // async mutex
    
    private static readonly ConcurrentDictionary<string, ISessionSource> Pools = new();
    private static readonly ConcurrentDictionary<string, ISessionSource> ImplicitPools = new();

    internal static async Task<ISession> GetSession(
        YdbConnectionStringBuilder settings,
        CancellationToken cancellationToken
    )
    {
        if (Pools.TryGetValue(settings.ConnectionString, out var sessionPool))
        {
            return await sessionPool.OpenSession(cancellationToken);
        }

        try
        {
            await SemaphoreSlim.WaitAsync(cancellationToken);

            if (Pools.TryGetValue(settings.ConnectionString, out var pool))
            {
                return await pool.OpenSession(cancellationToken);
            }

            var newSessionPool = new PoolingSessionSource<PoolingSession>(
                await PoolingSessionFactory.Create(settings), settings
            );

            Pools[settings.ConnectionString] = newSessionPool;

            return await newSessionPool.OpenSession(cancellationToken);
        }
        finally
        {
            SemaphoreSlim.Release();
        }
    }

    internal static ISession GetImplicitSession(YdbConnectionStringBuilder settings)
    {
        if (ImplicitPools.TryGetValue(settings.ConnectionString, out var ready))
            return ready.OpenSession(CancellationToken.None).GetAwaiter().GetResult();

        var driver = settings.BuildDriver().GetAwaiter().GetResult();
        ISessionSource source;

        SemaphoreSlim.Wait();
        try
        {
            if (!ImplicitPools.TryGetValue(settings.ConnectionString, out source))
            {
                source = new ImplicitSessionSource(driver);
                ImplicitPools[settings.ConnectionString] = source;
                driver = null;
            }
        }
        finally
        {
            SemaphoreSlim.Release();
            if (driver != null)
                driver.DisposeAsync().GetAwaiter().GetResult();
        }

        return source.OpenSession(CancellationToken.None).GetAwaiter().GetResult();
    }

    internal static async Task ClearPool(string connectionString)
    {
        Pools.TryRemove(connectionString, out var pooled);
        ImplicitPools.TryRemove(connectionString, out var implicitSrc);

        var tasks = new List<Task>(2);
        if (pooled != null)      tasks.Add(pooled.DisposeAsync().AsTask());
        if (implicitSrc != null) tasks.Add(implicitSrc.DisposeAsync().AsTask());

        if (tasks.Count > 0)
            await Task.WhenAll(tasks);
    }

    internal static async Task ClearAllPools()
    {
        var pooled = Pools.ToArray();
        var implicitArr = ImplicitPools.ToArray();

        Pools.Clear();
        ImplicitPools.Clear();

        var tasks = new List<Task>(pooled.Length + implicitArr.Length);
        tasks.AddRange(pooled.Select(kv => kv.Value.DisposeAsync().AsTask()));
        tasks.AddRange(implicitArr.Select(kv => kv.Value.DisposeAsync().AsTask()));
        await Task.WhenAll(tasks);
    }
}
