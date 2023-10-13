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
            const string query = @"
                DECLARE $is_what UTF8

                SELECT something
                FROM somewhere
                WHERE param = $param"; // idk if declare statement is needed

            var parameters = new Dictionary<string, YdbValue>
            {
                { "$param", YdbValue.MakeUtf8("...") }
            };

            var stream = session.ExecuteQueryYql(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit(),
                parameters: parameters
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
            const string queryString = @"
                DECLARE $is_what UTF8

                SELECT something
                FROM somewhere
                WHERE param = $param"; // idk if declare statement is needed

            var qb = new QueryBuilder(queryString)
                .WithParam("param", YdbValue.MakeUtf8("..."));


            var stream = session.ExecuteQueryYql(
                queryBuilder: qb,
                txControl: TxControl.BeginSerializableRW().Commit()
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
            const string queryString = @"
                DECLARE $is_what UTF8

                SELECT something
                FROM somewhere
                WHERE param = $param"; // idk if declare statement is needed

            var qb = new QueryBuilder(queryString)
                .WithParam("param", YdbValue.MakeUtf8("..."));

            var stream = session.ExecuteQueryYql(
                queryBuilder: qb,
                txControl: TxControl.BeginSerializableRW().Commit()
            );

            return stream;
        });

        // maybe it's impossible
        await foreach (var part in stream)
        {
        }
    }
}