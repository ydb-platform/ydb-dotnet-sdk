using Xunit;

namespace Ydb.Sdk.Tests.Fixture;

public class DriverFixture : IAsyncLifetime
{
    public Driver Driver { get; }

    protected DriverFixture()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        Driver = new Driver(driverConfig, Utils.GetLoggerFactory());
    }

    protected virtual void ClientDispose()
    {
    }

    public Task InitializeAsync() => Driver.Initialize();

    public Task DisposeAsync()
    {
        ClientDispose();

        return Driver.DisposeAsync().AsTask();
    }
}
