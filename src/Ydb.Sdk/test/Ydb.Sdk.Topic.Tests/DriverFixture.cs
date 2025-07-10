using Xunit;

namespace Ydb.Sdk.Topic.Tests;

public class DriverFixture : IAsyncLifetime
{
    public Driver Driver { get; }

    public DriverFixture()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        Driver = new Driver(driverConfig, Utils.LoggerFactory);
    }


    public Task InitializeAsync() => Driver.Initialize();

    public Task DisposeAsync() => Driver.DisposeAsync().AsTask();
}
