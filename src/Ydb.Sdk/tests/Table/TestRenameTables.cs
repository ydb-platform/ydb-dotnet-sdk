using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public sealed class TestRenameTables
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public TestRenameTables()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
    }

    [Fact]
    public async Task RenameNotExisting()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var renameTableItem = new RenameTableItem("source", "dest", false);

        var response = await tableClient.RenameTables(new[] { renameTableItem });
        Assert.Equal(StatusCode.SchemeError, response.Status.StatusCode);
    }

    [Fact]
    public async Task RenameTables()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var pairs = new List<(string, string)>();
        for (var i = 0; i < 5; i++)
        {
            pairs.Add(($"t{Guid.NewGuid():n}", $"t{Guid.NewGuid():n}"));
        }

        var items = new List<RenameTableItem>();
        foreach (var (source, dest) in pairs)
        {
            await Utils.CreateSimpleTable(tableClient, source);
            items.Add(new RenameTableItem(source, dest, false));
        }

        var response = await tableClient.RenameTables(items);
        response.EnsureSuccess();


        foreach (var (_, dest) in pairs)
        {
            await Utils.DropTable(tableClient, dest);
        }
    }
}
