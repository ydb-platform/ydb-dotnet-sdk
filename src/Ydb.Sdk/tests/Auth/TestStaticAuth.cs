using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Auth;

[Trait("Category", "Integration")]
public class TestStaticAuth : IDisposable
{
    // ReSharper disable once NotAccessedField.Local
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory? _loggerFactory;

    public TestStaticAuth(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = Utils.GetLoggerFactory();

        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local"
        );

        using var anonDriver = new Driver(driverConfig, _loggerFactory);
        anonDriver.Initialize().Wait();

        using var anonTableClient = new TableClient(anonDriver);

        Utils.ExecuteSchemeQuery(anonTableClient, "DROP USER IF EXISTS testuser", ensureSuccess: false).Wait();
        Utils.ExecuteSchemeQuery(anonTableClient, "CREATE USER testuser PASSWORD 'test_password'").Wait();
        Utils.ExecuteSchemeQuery(anonTableClient, "DROP USER IF EXISTS testnopass", ensureSuccess: false).Wait();
        Utils.ExecuteSchemeQuery(anonTableClient, "CREATE USER testnopass").Wait();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }


    [Fact]
    public async Task GoodAuth()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticCredentialsProvider("testuser", "test_password")
        );

        await using var driver = await Driver.CreateInitialized(driverConfig, _loggerFactory);

        using var tableClient = new TableClient(driver);

        var response = await Utils.ExecuteDataQuery(tableClient, "SELECT 1");
        var row = response.Result.ResultSets[0].Rows[0];
        Assert.Equal(1, row[0].GetInt32());
    }

    [Fact]
    public async Task WrongPass()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticCredentialsProvider("testuser", "wrong")
        );

        try
        {
            await using var driver = await Driver.CreateInitialized(driverConfig, _loggerFactory);
        }
        catch (AggregateException ae)
        {
            Assert.IsType<InvalidCredentialsException>(ae.InnerException);
        }
    }

    [Fact]
    public async Task NoPassAuth()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticCredentialsProvider("testnopass", "")
        );

        await using var driver = await Driver.CreateInitialized(driverConfig, _loggerFactory);

        using var tableClient = new TableClient(driver);

        var response = await Utils.ExecuteDataQuery(tableClient, "SELECT 1");
        var row = response.Result.ResultSets[0].Rows[0];
        Assert.Equal(1, row[0].GetInt32());
    }

    [Fact]
    public async Task NotExistAuth()
    {
        var driverConfig = new DriverConfig(
            endpoint: "grpc://localhost:2136",
            database: "/local",
            new StaticCredentialsProvider("notexists", "nopass")
        );

        try
        {
            await using var driver = await Driver.CreateInitialized(driverConfig, _loggerFactory);
        }
        catch (AggregateException ae)
        {
            Assert.IsType<InvalidCredentialsException>(ae.InnerException);
        }
    }
}