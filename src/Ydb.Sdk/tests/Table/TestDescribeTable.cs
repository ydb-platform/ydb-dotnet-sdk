using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class TestDescribeTable
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public TestDescribeTable()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
    }


    [Fact]
    public async Task DescribeNotExisting()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var response = await tableClient.DescribeTable("/local/not_exists");
        Assert.Equal(StatusCode.SchemeError, response.Status.StatusCode);
    }


    [Fact]
    public async Task CreateAndDescribe()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);

        var tablePath = $"t{Guid.NewGuid():n}";
        const string columnName = "myColumnName";

        await Utils.CreateSimpleTable(tableClient, tablePath, columnName);

        var describeResponse = await tableClient.DescribeTable(tablePath);
        describeResponse.Status.EnsureSuccess();
        Assert.True(describeResponse.Result.PrimaryKey.SequenceEqual(new[] { columnName }));

        await Utils.DropTable(tableClient, tablePath);
    }
}