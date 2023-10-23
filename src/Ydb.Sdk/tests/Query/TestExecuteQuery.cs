using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

[Trait("Category", "Integration")]
public class TestExecuteQuery
{
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

    public TestExecuteQuery()
    {
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

    public async Task ExecuteQueryTake3_1()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        await client.Query(
            query: "SELECT ...",
            parameters: new Dictionary<string, YdbValue>(),
            func: async stream =>
            {
                await foreach (var part in stream)
                {
                    part.EnsureSuccess();
                    /*
                     * some code
                     */
                }
            },
            txMode: TxMode.OnlineRO // default SerializableRW
        );

        await client.Tx(
            func: async tx =>
            {
                var stream = await tx.Query(
                    query: "SELECT ...",
                    parameters: new Dictionary<string, YdbValue>()
                );
                await foreach (var part in stream)
                {
                    part.EnsureSuccess();
                    /*
                     * some code
                     */
                }
            },
            txMode: TxMode.OnlineRO // default SerializableRW
        );
    }
}