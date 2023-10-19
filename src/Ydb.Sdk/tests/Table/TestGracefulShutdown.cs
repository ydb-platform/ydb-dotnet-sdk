using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class TestGracefulShutdown
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    private const string ShutdownUrl = "http://localhost:8765/actors/kqp_proxy?force_shutdown=all";

    public TestGracefulShutdown()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
    }

    [Fact]
    public async Task Test()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);


        var session1 = "";
        await tableClient.SessionExec(
            async session =>
            {
                session1 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        // _testOutputHelper.WriteLine(session1);
        await Task.Delay(1000);

        var session2 = "";
        await tableClient.SessionExec(
            async session =>
            {
                session2 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        // _testOutputHelper.WriteLine(session2);
        await Task.Delay(1000);


        // control check
        Assert.NotEqual("", session1);
        Assert.Equal(session1, session2);

        // SHUTDOWN
        using var httpClient = new HttpClient();
        await httpClient.GetAsync(ShutdownUrl);
        await Task.Delay(1000);

        // new session
        var session3 = "";
        await tableClient.SessionExec(
            async session =>
            {
                session3 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        Assert.Equal(session2, session3);

        var session4 = "";
        await tableClient.SessionExec(
            async session =>
            {
                session4 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        Assert.NotEqual("", session3);
        Assert.NotEqual(session3, session4);
    }
}