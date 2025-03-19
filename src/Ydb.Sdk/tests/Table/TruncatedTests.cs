using Xunit;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Tests.Fixture;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class TruncatedTests : IClassFixture<TableClientFixture>
{
    private readonly TableClientFixture _tableClientFixture;

    public TruncatedTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }


    [Fact]
    public async Task NotAllowTruncated()
    {
        const string query = "SELECT * FROM AS_TABLE($data)";
        var parameters = new Dictionary<string, YdbValue> { { "$data", MakeData(1001) } };

        await Assert.ThrowsAsync<TruncateException>(async () => await _tableClientFixture.TableClient
            .SessionExec(async session => await session.ExecuteDataQuery(
                    query: query,
                    parameters: parameters,
                    txControl: TxControl.BeginSerializableRW().Commit()
                )
            ));
    }

    [Fact]
    public async Task AllowTruncated()
    {
        const string query = "SELECT * FROM AS_TABLE($data)";
        var parameters = new Dictionary<string, YdbValue> { { "$data", MakeData(1001) } };
        var settings = new ExecuteDataQuerySettings { AllowTruncated = true };

        var response = await _tableClientFixture.TableClient.SessionExec(async session =>
            await session.ExecuteDataQuery(
                query: query,
                parameters: parameters,
                txControl: TxControl.BeginSerializableRW().Commit(),
                settings: settings)
        );

        Assert.True(response.Status.IsSuccess);
    }

    private static YdbValue MakeData(int n) => YdbValue.MakeList(
        Enumerable.Range(0, n)
            .Select(i => YdbValue.MakeStruct(
                new Dictionary<string, YdbValue>
                {
                    { "id", (YdbValue)i }
                }
            ))
            .ToList()
    );
}
