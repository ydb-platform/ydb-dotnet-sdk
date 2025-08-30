using System.Collections.Immutable;
using Xunit;

namespace Ydb.Sdk.Ado.Tests;

[CollectionDefinition("PoolManagerTests", DisableParallelization = true)]
public class PoolManagerTests
{
    [Theory]
    [InlineData(new[]
    {
        "MinSessionSize=1", "MinSessionSize=2", "MinSessionSize=3",
        "MinSessionSize=1;DisableDiscovery=True", "MinSessionSize=2;DisableDiscovery=True"
    }, 2, 5)] // 2 transports (by the DisableDiscovery flag), 5 pools
    [InlineData(
        new[] { "MinSessionSize=1", "MinSessionSize=2", "MinSessionSize=3", "MinSessionSize=4", "MinSessionSize=5" },
        1, 5)] // 1 transport, 5 five pools
    [InlineData(new[]
            { "MinSessionSize=1", "MinSessionSize=1", "MinSessionSize=2", "MinSessionSize=2", "MinSessionSize=3" }, 1,
        3)] // duplicate rows — we expect 1 transport, 3 pools
    [InlineData(new[]
    {
        "MinSessionSize=1;ConnectTimeout=5", "MinSessionSize=1;ConnectTimeout=6", "MinSessionSize=1;ConnectTimeout=7",
        "MinSessionSize=1;ConnectTimeout=8", "MinSessionSize=1;ConnectTimeout=9"
    }, 5, 5)] // 5 transport, 5 five pools
    [InlineData(new[]
    {
        "MinSessionSize=1"
    }, 1, 1)] // simple case
    public async Task PoolManager_CachingAndCleanup(string[] connectionStrings, int expectedDrivers, int expectedPools)
    {
        foreach (var (_, pool) in PoolManager.Pools)
        {
            await pool.DisposeAsync();
        }

        PoolManager.Pools.Clear();
        PoolManager.Drivers.Clear();

        var connections = connectionStrings
            .Select(connectionString => new YdbConnection(connectionString))
            .ToImmutableArray();
        var parallelTasks = connections.Select(connection => connection.OpenAsync()).ToList();
        await Task.WhenAll(parallelTasks);

        Assert.Equal(expectedDrivers, PoolManager.Drivers.Count);
        Assert.Equal(expectedPools, PoolManager.Pools.Count);

        await ClearAllConnections(connections);

        parallelTasks = connections.Select(connection => connection.OpenAsync()).ToList();
        await Task.WhenAll(parallelTasks);

        foreach (var (_, driver) in PoolManager.Drivers)
        {
            Assert.False(driver.IsDisposed);
        }

        Assert.Equal(expectedDrivers, PoolManager.Drivers.Count);
        Assert.Equal(expectedPools, PoolManager.Pools.Count);

        await ClearAllConnections(connections);
    }

    private static async Task ClearAllConnections(IReadOnlyCollection<YdbConnection> connections)
    {
        foreach (var connection in connections)
            await connection.CloseAsync();

        await YdbConnection.ClearAllPools();
        Assert.Empty(PoolManager.Pools);

        foreach (var (_, driver) in PoolManager.Drivers)
        {
            Assert.True(driver.IsDisposed);
        }
    }
}
