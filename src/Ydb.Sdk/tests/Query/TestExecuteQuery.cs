using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

[Trait("Category", "Integration")]
public class TestExecuteQuery
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );


    private const string Query = @"
                DECLARE $is_what UTF8

                SELECT something
                FROM somewhere
                WHERE param = $param"; // idk if declare statement is needed

    private readonly Dictionary<string, YdbValue> _parameters = new()
    {
        { "$param", YdbValue.MakeUtf8("...") }
    };

    public TestExecuteQuery(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<TestExecuteQuery>();
    }

    // public async Task ExecuteQueryTake1_1()
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     await queryClient.SessionExecStream(async session =>
    //     {
    //         var stream = session.ExecuteQueryYql(
    //             query: Query,
    //             tx: Tx.Begin().WithCommit(),
    //             parameters: _parameters
    //         );
    //
    //         while (await stream.Next())
    //         {
    //             var part = stream.Response;
    //             part.EnsureSuccess();
    //
    //             var resultSet = part.Result.ResultSet;
    //             if (resultSet?.Rows is null)
    //             {
    //                 continue;
    //             }
    //
    //             foreach (var row in resultSet.Rows)
    //             {
    //                 _logger.LogInformation(
    //                     "`something` find in `somewhere`:" +
    //                     $"\n\t`something`: {row["something"]}");
    //             }
    //         }
    //     });
    // }
    //
    // public async Task ExecuteQueryTake1_2()
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     var operationFunc = async (QuerySession session) =>
    //     {
    //         var stream = session.ExecuteQueryYql(
    //             query: Query,
    //             parameters: _parameters,
    //             tx: Tx.Begin().WithCommit()
    //         );
    //
    //         await foreach (var part in stream)
    //         {
    //             part.EnsureSuccess();
    //             //
    //             //
    //             //
    //         }
    //     };
    //
    //     await queryClient.SessionExecStream(
    //         operationFunc: operationFunc,
    //         retrySettings: new RetrySettings
    //         {
    //             IsIdempotent = true
    //         });
    // }
    //
    // public async Task ExecuteQueryTake1_3()
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     var stream = queryClient.SessionExecStream(session =>
    //     {
    //         var stream = session.ExecuteQueryYql(
    //             query: Query,
    //             parameters: _parameters,
    //             tx: Tx.Begin().WithCommit()
    //         );
    //
    //         return stream;
    //     });
    //
    //     // maybe it's impossible
    //     await foreach (var part in stream)
    //     {
    //     }
    // }
    //
    //
    // public async Task ExecuteQueryTake2_1() // transactions
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     var unused = await queryClient.SessionExec(async session =>
    //     {
    //         await using var tx = session.BeginTx(); // tx created on server 
    //
    //         var stream = session.ExecuteQueryYql(
    //             query: Query,
    //             parameters: _parameters,
    //             tx: Tx.Begin().WithCommit()
    //         );
    //
    //         await foreach (var part in stream)
    //         {
    //             part.EnsureSuccess();
    //             // something
    //         }
    //
    //         stream = session.ExecuteQueryYql(
    //             query: "...",
    //             tx); // i think same tx object can be used
    //         await foreach (var part in stream)
    //         {
    //             part.EnsureSuccess();
    //             // something
    //         }
    //
    //         var commitResponse = tx.Commit();
    //         commitResponse.EnsureSuccess();
    //         return commitResponse;
    //     });
    // }
    //
    //
    // public async Task ExecuteQueryTake2_2() // transactions auto rollback
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     var unused = await queryClient.SessionExec(async session =>
    //         await session.TxExec(async tx => // tx created on server 
    //             {
    //                 var stream = session.ExecuteQueryYql(
    //                     query: Query,
    //                     parameters: _parameters,
    //                     tx: tx
    //                 );
    //                 await foreach (var part in stream)
    //                 {
    //                     part.EnsureSuccess();
    //                     // something
    //                 }
    //
    //                 tx.Commit(); // rollback automatically if catch CommitException 
    //                 return stream.ExecStats;
    //             }
    //         )
    //     );
    // }
    //
    // public async Task ExecuteQueryTake2_3() // transactions 
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     var unused = await queryClient.SessionExecTx(async (session, tx) => //compact 
    //         {
    //             var stream = session.ExecuteQueryYql(
    //                 query: Query,
    //                 parameters: _parameters,
    //                 tx: tx
    //             );
    //             await foreach (var part in stream)
    //             {
    //                 part.EnsureSuccess();
    //                 // something
    //             }
    //
    //             tx.Commit(); // rollback automatically if catch CommitException 
    //             return stream.ExecStats;
    //         }
    //     );
    // }
    //
    // public async Task ExecuteQueryTake2_4() // transactions 
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var queryClient = new QueryClient(driver);
    //
    //     var unused = await queryClient.ExecTx( // create session and transaction on session; do retry
    //         async tx =>
    //         {
    //             var stream = tx.ExecuteQueryYql(
    //                 query: Query,
    //                 parameters: _parameters);
    //
    //             await foreach (var part in stream)
    //             {
    //                 part.EnsureSuccess();
    //                 // something
    //             }
    //
    //             return stream.ExecStats;
    //         },
    //         txMode: TxMode.OnlineRO, // default SerializableRW
    //         commit: true //  default false; automatic commit
    //     );
    // }

    private const string CreateString = @"CREATE TABLE series (
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
    
    [Fact]
    public async Task ExecuteQueryTake3_1()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var response = await client.Query(
            queryString: "SELECT 1",
            // parameters: new Dictionary<string, YdbValue>(),
            func: async stream =>
            {
                var result = new List<Dictionary<string, YdbValue>>();
                // await foreach (var part in stream)
                while (await stream.Next())
                {
                    var part = stream.Response;
                    part.EnsureSuccess();
                    var resultSet = part.ResultSet;
                    if (resultSet is not null)
                    {
                        foreach (var row in resultSet.Rows)
                        {
                            var resultPart = new Dictionary<string, YdbValue>();
                            var msg = "";
                            foreach (var column in resultSet.Columns)
                            {
                                resultPart[column.Name] = row[column.Name];
                                msg += $"{column.Name} = {row[column.Name]}; ";
                            }

                            result.Add(resultPart);
                            _testOutputHelper.WriteLine(msg);
                        }
                    }

                    _testOutputHelper.WriteLine(123.ToString());
                    /*
                     * some code
                     */
                }
                
                stream.Response.EnsureSuccess();

                return result;
            }
            // ,
            // txModeSettings: new TxModeOnlineSettings() // default SerializableRW
        );
        // await client.ExecOnTx(
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
}