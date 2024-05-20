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

        Driver = new Driver(driverConfig, Utils.GetLoggerFactory());
    }

    protected abstract void ClientDispose();

    public Task InitializeAsync()
    {
        return Driver.Initialize();
    }

    public Task DisposeAsync()
    {
        ClientDispose();

        return Driver.DisposeAsync().AsTask();
    }
}
