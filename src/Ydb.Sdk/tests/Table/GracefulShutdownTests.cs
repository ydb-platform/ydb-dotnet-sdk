using Xunit;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
[CollectionDefinition("GracefulShutdown isolation test", DisableParallelization = true)]
[Collection("GracefulShutdown isolation test")]
public class GracefulShutdownTests : IClassFixture<TableClientFixture>
{
    private const string ShutdownUrl = "http://localhost:8765/actors/kqp_proxy?force_shutdown=all";

    private readonly TableClientFixture _tableClientFixture;

    public GracefulShutdownTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }

    // [Fact]
    // https://github.com/ydb-platform/ydb-dotnet-sdk/issues/68
    public async Task Test()
    {
        var session1 = "";
        await _tableClientFixture.TableClient.SessionExec(
            async session =>
            {
                session1 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        var session2 = "";
        await _tableClientFixture.TableClient.SessionExec(
            async session =>
            {
                session2 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        // control check
        Assert.NotEqual("", session1);
        Assert.Equal(session1, session2);

        // SHUTDOWN
        using var httpClient = new HttpClient();
        await httpClient.GetAsync(ShutdownUrl);

        // new session
        var session3 = "";
        await _tableClientFixture.TableClient.SessionExec(
            async session =>
            {
                session3 = session.Id;
                return await session.ExecuteDataQuery("SELECT 1", TxControl.BeginSerializableRW().Commit());
            }
        );

        Assert.Equal(session2, session3);

        var session4 = "";
        await _tableClientFixture.TableClient.SessionExec(
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
