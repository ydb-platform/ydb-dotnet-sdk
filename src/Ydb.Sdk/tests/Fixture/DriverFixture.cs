using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Ydb.Sdk.Tests.Fixture;

public abstract class DriverFixture : IAsyncLifetime
{
    protected readonly Driver Driver;

    protected DriverFixture(DriverConfig? driverConfig = null)
    {
        driverConfig ??= new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        Driver = new Driver(driverConfig, GetLoggerFactory());
    }

    protected abstract void ClientDispose();

    public Task InitializeAsync() => Driver.Initialize();

    public Task DisposeAsync()
    {
        ClientDispose();

        return Driver.DisposeAsync().AsTask();
    }

    private static ILoggerFactory? GetLoggerFactory()
    {
        return new ServiceCollection()
            .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Information))
            .BuildServiceProvider()
            .GetService<ILoggerFactory>();
    }
}
