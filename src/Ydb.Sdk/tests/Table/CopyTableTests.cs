using Xunit;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class CopyTableTests : IClassFixture<TableClientFixture>
{
    private readonly TableClientFixture _tableClientFixture;

    public CopyTableTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }

    [Fact]
    public async Task CopyNotExisting()
    {
        const string source = "notExists";
        const string dest = "new";

        var response = await _tableClientFixture.TableClient.CopyTable(source, dest);
        Assert.Equal(StatusCode.SchemeError, response.Status.StatusCode);
    }

    [Fact]
    public async Task CopyTable()
    {
        var source = $"t{Guid.NewGuid():n}";
        var dest = $"t{Guid.NewGuid():n}";
        await Utils.CreateSimpleTable(_tableClientFixture.TableClient, source);

        var response = await _tableClientFixture.TableClient.CopyTable(source, dest);
        response.EnsureSuccess();

        await Utils.DropTable(_tableClientFixture.TableClient, source);
        await Utils.DropTable(_tableClientFixture.TableClient, dest);
    }

    [Fact]
    public async Task CopyTables()
    {
        var pairs = new List<(string, string)>();
        for (var i = 0; i < 5; i++)
        {
            pairs.Add(($"t{Guid.NewGuid():n}", $"t{Guid.NewGuid():n}"));
        }

        var items = new List<CopyTableItem>();
        foreach (var (source, dest) in pairs)
        {
            await Utils.CreateSimpleTable(_tableClientFixture.TableClient, source);
            items.Add(new CopyTableItem(source, dest, false));
        }

        var response = await _tableClientFixture.TableClient.CopyTables(items);
        response.EnsureSuccess();

        foreach (var (source, dest) in pairs)
        {
            await Utils.DropTable(_tableClientFixture.TableClient, source);
            await Utils.DropTable(_tableClientFixture.TableClient, dest);
        }
    }
}
