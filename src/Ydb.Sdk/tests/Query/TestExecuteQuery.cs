using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;
using RetrySettings = Ydb.Sdk.Services.Query.RetrySettings;

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

    [Fact]
    public async Task ExecuteQueryTake1()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);

        await queryClient.SessionExecStream(async session =>
        {
            var stream = session.ExecuteQueryYql(
                query: Query,
                tx: Tx.Begin().WithCommit(),
                parameters: _parameters
            );

            while (await stream.Next())
            {
                var part = stream.Response;
                part.EnsureSuccess();

                var resultSet = part.Result.ResultSet;
                if (resultSet?.Rows is null)
                {
                    continue;
                }

                foreach (var row in resultSet.Rows)
                {
                    _logger.LogInformation(
                        "`something` find in `somewhere`:" +
                        $"\n\t`something`: {row["something"]}");
                }
            }
        });
    }


    [Fact]
    public async Task ExecuteQueryTake2()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);

        var operationFunc = async (QuerySession session) =>
        {
            var stream = session.ExecuteQueryYql(
                query: Query,
                parameters: _parameters,
                tx: Tx.Begin().WithCommit()
            );

            await foreach (var part in stream)
            {
                part.EnsureSuccess();
                //
                //
                //
            }
        };

        await queryClient.SessionExecStream(
            operationFunc: operationFunc,
            retrySettings: new RetrySettings
            {
                IsIdempotent = true
            });
    }

    [Fact]
    public async Task ExecuteQueryTake3()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);

        var stream = queryClient.SessionExecStream(session =>
        {
            var stream = session.ExecuteQueryYql(
                query: Query,
                parameters: _parameters,
                tx: Tx.Begin().WithCommit()
            );

            return stream;
        });

        // maybe it's impossible
        await foreach (var part in stream)
        {
        }
    }


    [Fact]
    public async Task ExecuteQueryTake4() // transactions
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);

        await queryClient.SessionExec(async session =>
        {
            using var tx = session.BeginTx(); // tx created on server 

            var stream = session.ExecuteQueryYql(
                query: Query,
                parameters: _parameters,
                tx: tx
            );

            await foreach (var part in stream)
            {
                part.EnsureSuccess();
                // something
            }

            stream = session.ExecuteQueryYql(
                query: "...",
                tx); // i think same tx object can be used
            await foreach (var part in stream)
            {
                part.EnsureSuccess();
                // something
            }

            var commitResponse = tx.Commit();
            commitResponse.EnsureSuccess();
            return commitResponse;
        });
    }

    [Fact]
    public async Task ExecuteQueryTake5() // transactions auto rollback
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);

        await queryClient.SessionExec(async session =>
            await session.TxExec(async tx => // tx created on server 
                {
                    var stream = session.ExecuteQueryYql(
                        query: Query,
                        parameters: _parameters,
                        tx: tx
                    );
                    await foreach (var part in stream)
                    {
                        part.EnsureSuccess();
                        // something
                    }

                    tx.Commit(); // rollback automatically if catch CommitException 
                    return stream.ExecStats;
                }
            )
        );
    }

    [Fact]
    public async Task ExecuteQueryTake6() // transactions 
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);

        await queryClient.SessionExecTx(async (session, tx) => //compact 
            {
                var stream = session.ExecuteQueryYql(
                    query: Query,
                    parameters: _parameters,
                    tx: tx
                );
                await foreach (var part in stream)
                {
                    part.EnsureSuccess();
                    // something
                }

                tx.Commit(); // rollback automatically if catch CommitException 
                return stream.ExecStats;
            }
        );
    }
}