using Xunit;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class DescribeTableTests : IClassFixture<TableClientFixture>
{
    private readonly TableClientFixture _tableClientFixture;

    public DescribeTableTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }

    [Fact]
    public async Task DescribeNotExisting()
    {
        var response = await _tableClientFixture.TableClient.DescribeTable("/local/not_exists");
        Assert.Equal(StatusCode.SchemeError, response.Status.StatusCode);
    }


    [Fact]
    public async Task CreateAndDescribe()
    {
        var tablePath = $"t{Guid.NewGuid():n}";
        const string columnName = "myColumnName";

        await Utils.CreateSimpleTable(_tableClientFixture.TableClient, tablePath, columnName);

        var describeResponse = await _tableClientFixture.TableClient.DescribeTable(tablePath);
        describeResponse.Status.EnsureSuccess();
        Assert.True(describeResponse.Result.PrimaryKey.SequenceEqual(new[] { columnName }));

        await Utils.DropTable(_tableClientFixture.TableClient, tablePath);
    }
}
