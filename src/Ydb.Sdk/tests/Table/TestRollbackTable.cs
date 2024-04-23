using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Table;

public class TestRollbackTable
{
    private readonly ILoggerFactory _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    [Fact]
    public async Task RollbackTransactionTest()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var response = await tableClient.SessionExec(
            async session => await session.ExecuteSchemeQuery(@"
                CREATE TABLE `test` (
                    id Int32 NOT NULL,
                    name Text,
                    PRIMARY KEY (id)
                )")
        );
        response.Status.EnsureSuccess();

        response = await tableClient.SessionExec(async session =>
            {
                var res = await session.ExecuteDataQuery("UPSERT INTO test(id, name) VALUES (1, 'Example')",
                    TxControl.BeginSerializableRW());

                res.EnsureSuccess();

                return await session.RollbackTransaction(res.Tx!.TxId);
            }
        );

        response.Status.EnsureSuccess();

        response = await tableClient.SessionExec(async session =>
            await session.ExecuteDataQuery("SELECT COUNT(*) FROM test", TxControl.BeginSerializableRW().Commit())
        );

        response.Status.EnsureSuccess();

        Assert.Equal((ulong)0, ((ExecuteDataQueryResponse)response).Result.ResultSets[0].Rows[0][0].GetUint64());
    }
}
