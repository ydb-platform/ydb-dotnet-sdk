using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class TestCopyTable
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public TestCopyTable()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
    }

    [Fact]
    public async Task CopyNotExisting()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        const string source = "notExists";
        const string dest = "new";

        var response = await tableClient.CopyTable(source, dest);
        Assert.Equal(StatusCode.SchemeError, response.Status.StatusCode);
    }

    [Fact]
    public async Task CopyTable()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var source = $"t{Guid.NewGuid():n}";
        var dest = $"t{Guid.NewGuid():n}";
        await Utils.CreateSimpleTable(tableClient, source);

        var response = await tableClient.CopyTable(source, dest);
        response.EnsureSuccess();

        await Utils.DropTable(tableClient, source);
        await Utils.DropTable(tableClient, dest);
    }

    [Fact]
    public async Task CopyTables()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var pairs = new List<(string, string)>();
        for (var i = 0; i < 5; i++)
        {
            pairs.Add(($"t{Guid.NewGuid():n}", $"t{Guid.NewGuid():n}"));
        }

        var items = new List<CopyTableItem>();
        foreach (var (source, dest) in pairs)
        {
            await Utils.CreateSimpleTable(tableClient, source);
            items.Add(new CopyTableItem(source, dest, false));
        }

        var response = await tableClient.CopyTables(items);
        response.EnsureSuccess();


        foreach (var (source, dest) in pairs)
        {
            await Utils.DropTable(tableClient, source);
            await Utils.DropTable(tableClient, dest);
        }
    }
}
