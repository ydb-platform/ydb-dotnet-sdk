using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Tests.Query;

[Trait("Category", "Integration")]
public class TestSession
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public TestSession()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _loggerFactory.CreateLogger<TestExecuteQuery>();
    }

    [Fact]
    public async Task Create()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var response = await client.CreateSession();
        Assert.True(response.Status.IsSuccess);

        Assert.NotNull(response.Result);
    }

    [Fact]
    public async Task Delete()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var createResponse = await client.CreateSession();
        Assert.True(createResponse.Status.IsSuccess);

        var result = createResponse.Result;
        var session = result.Session;
        Assert.NotNull(session);

        var deleteResponse = await client.DeleteSession(session.Id);
        Assert.True(deleteResponse.Status.IsSuccess);
    }

    [Fact]
    public async Task Attach()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var createResponse = await client.CreateSession();
        Assert.True(createResponse.Status.IsSuccess);

        var result = createResponse.Result;
        var session = result.Session;
        Assert.NotNull(session);

        var sessionStateStream = client.AttachSession(session.Id);

        while (await sessionStateStream.Next())
        {
            Assert.True(sessionStateStream.Response.Status.IsSuccess);
            break;
        }
    }
}