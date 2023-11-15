using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

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


    [Fact]
    public async Task TestSchemeQuery()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var createResponse = await client.Exec("CREATE TABLE demo_table (id Int32, data Text, PRIMARY KEY(id));");
        Assert.Equal(StatusCode.Success, createResponse.Status.StatusCode);
        var dropResponse = await client.Exec("DROP TABLE demo_table;");
        Assert.Equal(StatusCode.Success, dropResponse.Status.StatusCode);
    }

    [Fact]
    public async Task TestSimpleSelect()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        var response = await client.Query("SELECT 2 + 3", async stream =>
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
        Assert.Equal(StatusCode.Success, response.Status.StatusCode);
        Assert.NotNull(response.Result);
        if (response.Result != null)
        {
            Assert.Single(response.Result);
            Assert.True(response.Result[0][0].GetInt32() == 5);
        }
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

    [Fact]
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
                    WHERE is_valid = true
                    ORDER BY name DESC
                    LIMIT 1;";
                var selectResponse = await tx.Query(selectQuery, async stream =>
                {
                    var result = new List<Entity>();
                    await foreach (var part in stream)
                    {
                        Assert.Equal(StatusCode.Success, part.Status.StatusCode);
                        if (part.ResultSet != null)
                        {
                            result.AddRange(
                                part.ResultSet.Rows.Select(row => new Entity(
                                    Id: row["id"].GetInt32(),
                                    Name: row["name"].GetOptionalUtf8()!,
                                    Payload: row["payload"].GetOptionalString()!,
                                    IsValid: (bool)row["is_valid"].GetOptionalBool()!)
                                )
                            );
                        }
                    }

                    return result;
                });
                Assert.Equal(StatusCode.Success, selectResponse.Status.StatusCode);

                var entityToDelete = selectResponse.Result![0];

                const string deleteQuery = @$"
                    DELETE FROM {tableName}
                    WHERE id = $id
                ";

                var deleteParameters = new Dictionary<string, YdbValue>
                {
                    { "$id", (YdbValue)entityToDelete.Id }
                };

                var deleteResponse = await tx.Exec(deleteQuery, deleteParameters);
                Assert.Equal(StatusCode.Success, deleteResponse.Status.StatusCode);
            }
        );
        Assert.Equal(StatusCode.Success, response.Status.StatusCode);

        await DropTable(tableClient, tableName);
    }
}