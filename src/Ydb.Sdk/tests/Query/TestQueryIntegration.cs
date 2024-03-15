using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

// TODO: Fix flap tests
// https://github.com/ydb-platform/ydb-dotnet-sdk/issues/66
[Trait("Category", "Integration")]
public class TestQueryIntegration
{
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public TestQueryIntegration()
    {
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _loggerFactory.CreateLogger<TestQueryIntegration>();
    }

    // TODO uncomment when DDL support will be available on the server side
    // [Fact]
    // public async Task TestSchemeQuery()
    // {
    //     await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
    //     using var client = new QueryClient(driver);
    //
    //     var createResponse = await client.Exec("CREATE TABLE demo_table (id Int32, data Text, PRIMARY KEY(id));");
    //     Assert.Equal(StatusCode.Success, createResponse.Status.StatusCode);
    //     var dropResponse = await client.Exec("DROP TABLE demo_table;",
    //         retrySettings: new RetrySettings { IsIdempotent = false });
    //     Assert.Equal(StatusCode.Success, dropResponse.Status.StatusCode);
    // }

    // [Fact]
    // https://github.com/ydb-platform/ydb-dotnet-sdk/issues/66
    public async Task TestSimpleSelect()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        const string queryString = "SELECT 2 + 3 AS sum";

        var responseQuery = await client.Query(queryString, async stream =>
        {
            var rows = new List<Sdk.Value.ResultSet.Row>();
            await foreach (var part in stream)
            {
                Assert.Equal(StatusCode.Success, part.Status.StatusCode);
                if (part.ResultSet != null)
                {
                    rows.AddRange(part.ResultSet.Rows);
                }
            }

            return rows;
        });

        var responseAllRows = await client.ReadAllRows(queryString);
        var responseSingleRow = await client.ReadSingleRow(queryString); // ReadOneRow

        Assert.Equal(StatusCode.Success, responseQuery.Status.StatusCode);
        Assert.Equal(StatusCode.Success, responseAllRows.Status.StatusCode);
        Assert.Equal(StatusCode.Success, responseSingleRow.Status.StatusCode);
        Assert.NotNull(responseQuery.Result);
        Assert.NotNull(responseSingleRow.Result);
        Assert.NotNull(responseAllRows.Result);
        Assert.Single(responseQuery.Result!);
        Assert.Single(responseAllRows.Result!);

        var valueQuery = (int)responseQuery.Result!.First()["sum"];
        var valueReadAll = (int)responseAllRows.Result!.First()["sum"];
        var valueReadSingle = (int)responseSingleRow.Result!["sum"];

        Assert.Equal(valueQuery, valueReadAll);
        Assert.Equal(valueQuery, valueReadSingle);
        Assert.Equal(5, valueQuery);
    }


    private record Entity(int Id, string Name, byte[] Payload, bool IsValid);

    private async Task InitEntityTable(TableClient client, string tableName)
    {
        var query = $@"
        CREATE TABLE `{tableName}` (
            id Int32 NOT NULL,
            name Utf8,
            payload String,
            is_valid Bool,
            PRIMARY KEY (id)
        )";

        await ExecSchemeQueryOnTableClient(client, query);
    }

    private async Task DropTable(TableClient client, string tableName)
    {
        var query = $"DROP TABLE `{tableName}`";
        await ExecSchemeQueryOnTableClient(client, query);
    }

    private async Task ExecSchemeQueryOnTableClient(TableClient client, string query)
    {
        var response = await client.SessionExec(
            async session => await session.ExecuteSchemeQuery(query)
        );
        response.Status.EnsureSuccess();
    }

    // [Fact]
    public async Task TestSimpleCrud()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);
        using var tableClient = new TableClient(driver);

        const string tableName = "crudTable";
        await InitEntityTable(tableClient, tableName);

        var entities = new List<Entity>
        {
            new(1, "entity 1", Array.Empty<byte>(), true),
            new(2, "entity 2", Array.Empty<byte>(), true),
            new(3, "entity 3", new byte[] { 0x00, 0x22 }, true),
            new(3, "duplicate", new byte[] { 0x00, 0x22 }, false),
            new(5, "entity 5", new byte[] { 0x12, 0x23, 0x34, 0x45, 0x56 }, false)
        };

        const string upsertQuery = @$"
            UPSERT INTO `{tableName}` (id, name, payload, is_valid) 
            VALUES ($id, $name, $payload, $is_valid)
        ";

        foreach (var entity in entities)
        {
            var parameters = new Dictionary<string, YdbValue>
            {
                { "$id", (YdbValue)entity.Id },
                { "$name", YdbValue.MakeUtf8(entity.Name) },
                { "$payload", YdbValue.MakeString(entity.Payload) },
                { "$is_valid", (YdbValue)entity.IsValid }
            };
            var upsertResponse = await client.Exec(upsertQuery, parameters);
            Assert.Equal(StatusCode.Success, upsertResponse.Status.StatusCode);
        }

        var response = await client.DoTx(async tx =>
            {
                const string selectQuery = @$"
                    SELECT * FROM {tableName}
                    ORDER BY name DESC
                    LIMIT 1;";
                var selectResponse = await tx.ReadSingleRow(selectQuery);
                Assert.Equal(StatusCode.Success, selectResponse.Status.StatusCode);

                var entityId = selectResponse.Result!["id"];

                const string deleteQuery = @$"
                    DELETE FROM {tableName}
                    WHERE id = $id
                ";

                var deleteParameters = new Dictionary<string, YdbValue>
                {
                    { "$id", entityId }
                };

                var deleteResponse = await tx.Exec(deleteQuery, deleteParameters);
                Assert.Equal(StatusCode.Success, deleteResponse.Status.StatusCode);
            }
        );
        Assert.Equal(StatusCode.Success, response.Status.StatusCode);

        await DropTable(tableClient, tableName);
    }

    [Fact]
    public async Task TestDoTxRollback()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var response = await client.DoTx(_ =>
        {
            var response = new ClientInternalErrorResponse("test rollback if status unsuccessful");
            response.EnsureSuccess();
            return Task.CompletedTask;
        });
        Assert.Equal(StatusCode.ClientInternalError, response.Status.StatusCode);


        response = await client.DoTx(_ => throw new ArithmeticException("2 + 2 = 5"));
        Assert.Equal(StatusCode.ClientInternalError, response.Status.StatusCode);
    }

    // [Theory]
    // [InlineData(StatusCode.ClientInternalError, StatusCode.Success, 2, true)]
    // [InlineData(StatusCode.ClientInternalError, StatusCode.ClientInternalError, 1, false)]
    // [InlineData(StatusCode.InternalError, StatusCode.InternalError, 1, true)]
    // [InlineData(StatusCode.Aborted, StatusCode.Success, 2, false)]
    // https://github.com/ydb-platform/ydb-dotnet-sdk/issues/66
    public async Task TestIdempotency(StatusCode statusCode, StatusCode expectedStatusCode, int expectedAttempts,
        bool isIdempotent)
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var attempts = 0;
        var response = await client.Query("SELECT 1", async stream =>
            {
                attempts += 1;
                var rows = new List<Sdk.Value.ResultSet.Row>();
                await foreach (var part in stream)
                {
                    if (part.ResultSet is not null)
                    {
                        rows.AddRange(part.ResultSet.Rows);
                    }
                }

                if (attempts == 1)
                {
                    throw new StatusUnsuccessfulException(new Status(statusCode, "test idempotency"));
                }

                return rows;
            },
            retrySettings: new RetrySettings { IsIdempotent = isIdempotent });

        Assert.Equal(expectedStatusCode, response.Status.StatusCode);
        Assert.Equal(expectedAttempts, attempts);
    }

    // [Fact]
    // https://github.com/ydb-platform/ydb-dotnet-sdk/issues/66
    public async Task TestReaders()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var queryClient = new QueryClient(driver);
        using var tableClient = new TableClient(driver);
        const string tableName = "readTable";
        await InitEntityTable(tableClient, tableName);


        const string upsertQuery = @$"
            UPSERT INTO `{tableName}` (id, name, payload, is_valid) 
            VALUES ($id, $name, $payload, $is_valid)
        ";
        var entities = new List<Entity>
        {
            new(1, "entity 1", Array.Empty<byte>(), true),
            new(2, "entity 2", Array.Empty<byte>(), true)
        };
        foreach (var entity in entities)
        {
            var parameters = new Dictionary<string, YdbValue>
            {
                { "$id", (YdbValue)entity.Id },
                { "$name", YdbValue.MakeUtf8(entity.Name) },
                { "$payload", YdbValue.MakeString(entity.Payload) },
                { "$is_valid", (YdbValue)entity.IsValid }
            };
            var upsertResponse = await queryClient.Exec(upsertQuery, parameters);
            Assert.Equal(StatusCode.Success, upsertResponse.Status.StatusCode);
        }

        const string SelectMultipleResultSetsQuery = @$"
                SELECT name FROM {tableName};
                SELECT * FROM {tableName}
            ";

        const string SelectMultipleRowsQuery = @$"
                SELECT * FROM {tableName}
            ";
        const string SelectSingleRowQuery = @$"
                SELECT * FROM {tableName} LIMIT 1
            ";
        const string SelectScalarQuery = @$"
                SELECT name FROM {tableName} LIMIT 1
            ";

        {
            var response = await queryClient.ReadAllResultSets(SelectMultipleResultSetsQuery);
            Assert.Equal(StatusCode.Success, response.Status.StatusCode);
            var resultSets = response.Result!;
            Assert.Equal(2, resultSets.Count);
            Assert.Equal(2, resultSets[0].Count);
        }

        {
            await Assert.ThrowsAsync<QueryWrongResultFormatException>(() =>
                queryClient.ReadAllRows(SelectMultipleResultSetsQuery));
            var response = await queryClient.ReadAllRows(SelectMultipleRowsQuery);
            Assert.Equal(StatusCode.Success, response.Status.StatusCode);
            var resultSet = response.Result;
            Assert.NotNull(resultSet);
            Assert.Equal(2, resultSet!.Count);
        }

        {
            await Assert.ThrowsAsync<QueryWrongResultFormatException>(() =>
                queryClient.ReadSingleRow(SelectMultipleResultSetsQuery));
            await Assert.ThrowsAsync<QueryWrongResultFormatException>(() =>
                queryClient.ReadSingleRow(SelectMultipleRowsQuery));
            var response = await queryClient.ReadSingleRow(SelectSingleRowQuery);
            var resultSet = response.Result;
            Assert.NotNull(resultSet);
        }

        {
            await Assert.ThrowsAsync<QueryWrongResultFormatException>(() =>
                queryClient.ReadScalar(SelectMultipleResultSetsQuery));
            await Assert.ThrowsAsync<QueryWrongResultFormatException>(() =>
                queryClient.ReadScalar(SelectMultipleRowsQuery));
            await Assert.ThrowsAsync<QueryWrongResultFormatException>(() =>
                queryClient.ReadScalar(SelectSingleRowQuery));
            var response = await queryClient.ReadScalar(SelectScalarQuery);
            var resultSet = response.Result;
            Assert.NotNull(resultSet);
        }

        await DropTable(tableClient, tableName);
    }
}
