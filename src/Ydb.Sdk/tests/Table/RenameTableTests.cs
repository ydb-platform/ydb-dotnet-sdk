using Xunit;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public sealed class RenameTableTests : IClassFixture<TableClientFixture>
{
    private readonly TableClientFixture _tableClientFixture;

    public RenameTableTests(TableClientFixture tableClientFixture)
    {
        _tableClientFixture = tableClientFixture;
    }

    [Fact]
    public async Task RenameNotExisting()
    {
        var renameTableItem = new RenameTableItem("source", "dest", false);

        var response = await _tableClientFixture.TableClient.RenameTables(new[] { renameTableItem });
        Assert.Equal(StatusCode.SchemeError, response.Status.StatusCode);
    }

    [Fact]
    public async Task RenameTables()
    {
        var pairs = new List<(string, string)>();
        for (var i = 0; i < 5; i++)
        {
            pairs.Add(($"t{Guid.NewGuid():n}", $"t{Guid.NewGuid():n}"));
        }

        var items = new List<RenameTableItem>();
        foreach (var (source, dest) in pairs)
        {
            await Utils.CreateSimpleTable(_tableClientFixture.TableClient, source);
            items.Add(new RenameTableItem(source, dest, false));
        }

        var response = await _tableClientFixture.TableClient.RenameTables(items);
        response.EnsureSuccess();


        foreach (var (_, dest) in pairs)
        {
            await Utils.DropTable(_tableClientFixture.TableClient, dest);
        }
    }
}
