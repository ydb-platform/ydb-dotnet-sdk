using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Auth;

[Trait("Category", "AuthStatic")]
public class TestStaticAuth
{
    // ReSharper disable once NotAccessedField.Local
    private readonly ITestOutputHelper _output;

    public TestStaticAuth(ITestOutputHelper output)
    {
        _output = output;
    }

    private static ServiceProvider GetServiceProvider()
    {
        return new ServiceCollection()
            .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Information))
            .BuildServiceProvider();
    }

    [Fact]
    public static async Task GoodAuth()
    {
        var serviceProvider = GetServiceProvider();
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticProvider("testuser", "test_password")
        );

        await using var driver = await Driver.CreateInitialized(driverConfig, loggerFactory);

        using var tableClient = new TableClient(driver);

        var response = await Utils.ExecuteDataQuery(tableClient, "SELECT 1");
        var row = response.Result.ResultSets[0].Rows[0];
        Assert.Equal(1, row[0].GetInt32());
    }

    [Fact]
    public static async Task WrongAuth()
    {
        var serviceProvider = GetServiceProvider();
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticProvider("nouser", "nopass")
        );

        await Assert.ThrowsAsync<AggregateException>(async delegate
        {
            await using var driver = await Driver.CreateInitialized(driverConfig, loggerFactory);
        });
    }
}