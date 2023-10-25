using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Query;

[Trait("Category", "Integration")]
public class TestExecuteQuery
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    private const string CreateTableQuery = @"CREATE TABLE series (
            series_id Uint64 NOT NULL,
            title Utf8,
            series_info Utf8,
            release_date Date,
            PRIMARY KEY (series_id)
        );

        CREATE TABLE seasons (
            series_id Uint64,
            season_id Uint64,
            title Utf8,
            first_aired Date,
            last_aired Date,
            PRIMARY KEY (series_id, season_id)
        );

        CREATE TABLE episodes (
            series_id Uint64,
            season_id Uint64,
            episode_id Uint64,
            title Utf8,
            air_date Date,
            PRIMARY KEY (series_id, season_id, episode_id)
        );";

    public TestExecuteQuery(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _loggerFactory.CreateLogger<TestExecuteQuery>();
    }


    [Fact]
    public async Task Query()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);
        using var tableClient = new TableClient(driver);

        // it seems to ddl not working in query service ( or im doing something wrong)
        await Utils.ExecuteSchemeQuery(tableClient, CreateTableQuery, ensureSuccess: false);
        // var createResponse = await client.Query(
        //     CreateString,
        //     async stream =>
        //     {
        //         while (await stream.Next())
        //         {
        //             var part = stream.Response;
        //             part.EnsureSuccess();
        //         }
        //     },
        // );
        // createResponse.EnsureSuccess();

        var fillResponse = await client.Query(@"
                    REPLACE INTO series
                    SELECT * FROM AS_TABLE($seriesData);

                    REPLACE INTO seasons
                    SELECT * FROM AS_TABLE($seasonsData);

                    REPLACE INTO episodes
                    SELECT * FROM AS_TABLE($episodesData);",
            parameters: QueryUtils.GetDataParams(),
            func: async stream =>
            {
                while (await stream.Next())
                {
                    stream.Response.EnsureSuccess();
                }
            }
        );
        fillResponse.EnsureSuccess();

        var response = await client.Query(
            queryString: "SELECT * FROM series",
            // parameters: new Dictionary<string, YdbValue>(),
            func: async stream =>
            {
                var series = new List<Series>();
                // await foreach (var part in stream) // TODO
                while (await stream.Next())
                {
                    var part = stream.Response;
                    part.EnsureSuccess();
                    var resultSet = part.ResultSet;
                    if (resultSet is not null)
                    {
                        series.AddRange(resultSet.Rows.Select(Series.FromRow));
                    }
                }

                stream.Response.EnsureSuccess();

                return series;
            }
            // txModeSettings: new TxModeOnlineSettings() // default SerializableRW
        );

        response.EnsureSuccess();
        if (response.Result != null)
        {
            foreach (var series in response.Result)
            {
                _testOutputHelper.WriteLine(series.ToString());
            }
        }
    }
    
    // await client.ExecOnTx( // TODO
    //     func: async tx =>
    //     {
    //         var stream = await tx.Query(
    //             query: "SELECT ...",
    //             parameters: new Dictionary<string, YdbValue>()
    //         );
    //         await foreach (var part in stream)
    //         {
    //             part.EnsureSuccess();
    //             /*
    //              * some code
    //              */
    //         }
    //     },
    //     txModeSettings: new TxModeOnlineSettings() // default SerializableRW
    // );
}