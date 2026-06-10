using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests;

public class DriverOwnershipTests
{
    private static readonly FieldInfo OwnerCountField =
        typeof(BaseDriver).GetField("_ownerCount", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public async Task BaseDriver_ConcurrentDisposeAndRegisterOwner_NoDisposedWithOwners()
    {
        const int iterations = 2000;

        for (var i = 0; i < iterations; i++)
        {
            var driver = new TestDriver();
            Assert.True(driver.RegisterOwner());

            var registerTask = Task.Run(() => driver.RegisterOwner());
            var disposeTask = Task.Run(() => driver.DisposeAsync().AsTask());

            await Task.WhenAll(registerTask, disposeTask);

            var ownerCount = (int)OwnerCountField.GetValue(driver)!;
            var disposed = driver.IsDisposed;

            Assert.True(ownerCount >= 0);
            Assert.False(disposed && ownerCount > 0);

            if (registerTask is { Result: true }) continue;
            Assert.True(disposed);
            Assert.Equal(0, ownerCount);
        }
    }

    private sealed class TestDriver() : BaseDriver(
        new DriverConfig(false, "localhost", 2136, "/local"),
        NullLoggerFactory.Instance,
        NullLoggerFactory.Instance.CreateLogger<TestDriver>()
    )
    {
        protected override EndpointInfo GetEndpoint(long nodeId) =>
            new(0, false, "localhost", 2136, string.Empty);

        protected override void OnRpcError(EndpointInfo endpointInfo, RpcException e)
        {
        }
    }
}
