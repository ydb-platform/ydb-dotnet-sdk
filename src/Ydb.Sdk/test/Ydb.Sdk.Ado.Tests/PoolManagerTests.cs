using System.Collections.Immutable;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Ydb.Sdk.Ado.Tests;

[Collection("DisableParallelization")]
public class PoolManagerTests
{
    [Theory]
    [InlineData(new[]
    {
        "MinPoolSize=1", "MinPoolSize=2", "MinPoolSize=3",
        "MinPoolSize=1;DisableDiscovery=True", "MinPoolSize=2;DisableDiscovery=True"
    }, 2, 5)] // 2 transports (by the DisableDiscovery flag), 5 pools
    [InlineData(
        new[] { "MinPoolSize=1", "MinPoolSize=2", "MinPoolSize=3", "MinPoolSize=4", "MinPoolSize=5" },
        1, 5)] // 1 transport, 5 five pools
    [InlineData(new[]
            { "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=3" }, 1,
        3)] // duplicate rows — we expect 1 transport, 3 pools
    [InlineData(new[]
    {
        "MinPoolSize=1;ConnectTimeout=5", "MinPoolSize=1;ConnectTimeout=6", "MinPoolSize=1;ConnectTimeout=7",
        "MinPoolSize=1;ConnectTimeout=8", "MinPoolSize=1;ConnectTimeout=9"
    }, 5, 5)] // 5 transport, 5 five pools
    [InlineData(new[] { "MinPoolSize=1" }, 1, 1)] // simple case
    [InlineData(new[]
    {
        "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1",
        "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1",
        "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1",
        "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1", "MinPoolSize=1",
        "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2",
        "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2",
        "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=2", "MinPoolSize=3",
        "MinPoolSize=3", "MinPoolSize=3", "MinPoolSize=3", "MinPoolSize=3", "MinPoolSize=3"
    }, 1, 3)] // duplicate rows — we expect 1 transport, 3 pools, stress test
    public async Task PoolManager_CachingAndCleanup(string[] connectionStrings, int expectedDrivers, int expectedPools)
    {
        await YdbConnection.ClearAllPools();
        foreach (var (_, driver) in PoolManager.Drivers)
            Assert.True(driver.IsDisposed);
        PoolManager.Drivers.Clear();

        var connections = connectionStrings
            .Select(connectionString => new YdbConnection(connectionString))
            .ToImmutableArray();
        await Task.WhenAll(connections.Select(connection => connection.OpenAsync()));

        Assert.Equal(expectedDrivers, PoolManager.Drivers.Count);
        Assert.Equal(expectedPools, PoolManager.Pools.Count);

        await ClearAllConnections(connections);
        await Task.WhenAll(connections.Select(connection => connection.OpenAsync()));

        foreach (var (_, driver) in PoolManager.Drivers)
            Assert.False(driver.IsDisposed);

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

    [Fact]
    public async Task PoolManager_GetDriver_RecreatesWhenRegisterFails()
    {
        PoolManager.Drivers.Clear();
        PoolManager.Pools.Clear();

        var driver1 = new FakeDriver(registerOwnerResult: false, isDisposed: true);
        var driver2 = new FakeDriver(registerOwnerResult: true, isDisposed: false);
        var factory = new FakeDriverFactory("grpc://race", driver1, driver2);

        var driver = await PoolManager.GetDriver(factory);

        Assert.Same(driver2, driver);
        Assert.Equal(2, factory.CreateCalls);
        Assert.Same(driver2, PoolManager.Drivers[factory.GrpcConnectionString]);
        Assert.Equal(1, driver1.RegisterCalls);
        Assert.Equal(1, driver2.RegisterCalls);
    }

    private sealed class FakeDriverFactory(string grpcConnectionString, params IDriver[] drivers) : IDriverFactory
    {
        private readonly Queue<IDriver> _drivers = new(drivers);

        public int CreateCalls { get; private set; }

        public Task<IDriver> CreateAsync()
        {
            CreateCalls++;
            return Task.FromResult(_drivers.Dequeue());
        }

        public string GrpcConnectionString { get; } = grpcConnectionString;

        public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
    }

    private sealed class FakeDriver(bool registerOwnerResult, bool isDisposed) : IDriver
    {
        public int RegisterCalls { get; private set; }

        public Task<TResponse> UnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            TRequest request,
            GrpcRequestSettings settings)
            where TRequest : class
            where TResponse : class =>
            throw new NotImplementedException();

        public ValueTask<IServerStream<TResponse>> ServerStreamCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            TRequest request,
            GrpcRequestSettings settings)
            where TRequest : class
            where TResponse : class =>
            throw new NotImplementedException();

        public ValueTask<IBidirectionalStream<TRequest, TResponse>> BidirectionalStreamCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            GrpcRequestSettings settings)
            where TRequest : class
            where TResponse : class =>
            throw new NotImplementedException();

        public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;

        public bool RegisterOwner()
        {
            RegisterCalls++;
            return registerOwnerResult;
        }

        public bool IsDisposed { get; private set; } = isDisposed;

        public string Database => "db";

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
