using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Table;

[Trait("Category", "Integration")]
public class Truncated
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public Truncated()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
    }


    [Fact]
    public async Task NotAllowTruncated()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);


        const string query = "SELECT * FROM AS_TABLE($data)";
        var parameters = new Dictionary<string, YdbValue> { { "$data", MakeData(1001) } };

        await Assert.ThrowsAsync<TruncateException>(async () => await tableClient.SessionExec(async session =>
            await session.ExecuteDataQuery(
                query: query,
                parameters: parameters,
                txControl: TxControl.BeginSerializableRW().Commit()
            )
        ));
    }

    [Fact]
    public async Task AllowTruncated()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var tableClient = new TableClient(driver);


        const string query = "SELECT * FROM AS_TABLE($data)";
        var parameters = new Dictionary<string, YdbValue> { { "$data", MakeData(1001) } };
        var settings = new ExecuteDataQuerySettings { AllowTruncated = true };

        var response = await tableClient.SessionExec(async session =>
            await session.ExecuteDataQuery(
                query: query,
                parameters: parameters,
                txControl: TxControl.BeginSerializableRW().Commit(),
                settings: settings)
        );

        Assert.True(response.Status.IsSuccess);
    }

    private static YdbValue MakeData(int n)
    {
        return YdbValue.MakeList(
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
}
