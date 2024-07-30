using Xunit;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class RollbackTableTests : IClassFixture<TableClientFixture>
{
    private readonly TableClientFixture _tableClientFixture;

    public RollbackTableTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }

    [Fact]
    public async Task RollbackTransactionTest()
    {
        var response = await _tableClientFixture.TableClient.SessionExec(
            async session => await session.ExecuteSchemeQuery(@"
                CREATE TABLE `test` (
                    id Int32 NOT NULL,
                    name Text,
                    PRIMARY KEY (id)
                )")
        );
        response.Status.EnsureSuccess();

        response = await _tableClientFixture.TableClient.SessionExec(async session =>
            {
                var res = await session.ExecuteDataQuery("UPSERT INTO test(id, name) VALUES (1, 'Example')",
                    TxControl.BeginSerializableRW());

                res.EnsureSuccess();

                return await session.RollbackTransaction(res.Tx!.TxId);
            }
        );

        response.Status.EnsureSuccess();

        response = await _tableClientFixture.TableClient.SessionExec(async session =>
            await session.ExecuteDataQuery("SELECT COUNT(*) FROM test", TxControl.BeginSerializableRW().Commit())
        );

        response.Status.EnsureSuccess();

        Assert.Equal((ulong)0, ((ExecuteDataQueryResponse)response).Result.ResultSets[0].Rows[0][0].GetUint64());
    }
}
