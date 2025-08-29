using Xunit;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Value;

public class YdbListTests : TestBase
{
    private static async Task WithTempTableAsync(
        YdbConnection conn,
        string namePrefix,
        string columns,
        Func<string, Task> body)
    {
        var table = $"{namePrefix}_{Guid.NewGuid():N}";
        var createSql = $"CREATE TABLE {table} (\n{columns}\n)";
        var dropSql = $"DROP TABLE {table}";

        await using (var create = conn.CreateCommand())
        {
            create.CommandText = createSql;
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            await body(table);
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = dropSql;
            await drop.ExecuteNonQueryAsync();
        }
    }

    private static async Task ExecAsTableAsync(
        YdbConnection conn,
        string verb,
        string table,
        string paramName,
        YdbList rows)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"{verb} INTO {table}\nSELECT * FROM AS_TABLE({paramName});";
        cmd.Parameters.Add(new YdbParameter(paramName, rows));
        await cmd.ExecuteNonQueryAsync();
    }

    private new static async Task<int> CountAsync(YdbConnection conn, string table)
    {
        await using var check = conn.CreateCommand();
        check.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt32(await check.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Insert_With_YdbList_Works()
    {
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_insert",
            """
            Id Int64,
            Value Text,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                var rows = YdbList.Struct("Id", "Value")
                    .AddRow(1L, "a")
                    .AddRow(2L, "b");

                await ExecAsTableAsync(conn, "INSERT", table, "$rows", rows);

                Assert.Equal(2, await CountAsync(conn, table));
            });
    }

    [Fact]
    public async Task YdbList_WhenUpsertOperation_InsertAndUpdate()
    {
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_upsert",
            """
            Id Int64,
            Value Text,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                await using (var seed = conn.CreateCommand())
                {
                    seed.CommandText = $"INSERT INTO {table} (Id, Value) VALUES (1, 'old')";
                    await seed.ExecuteNonQueryAsync();
                }

                var rows = YdbList.Struct("Id", "Value")
                    .AddRow(1L, "new")
                    .AddRow(2L, "two");

                await ExecAsTableAsync(conn, "UPSERT", table, "$rows", rows);

                await using (var check = conn.CreateCommand())
                {
                    check.CommandText = $"SELECT Value FROM {table} WHERE Id=1";
                    Assert.Equal("new", (string)(await check.ExecuteScalarAsync())!);

                    check.CommandText = $"SELECT Value FROM {table} WHERE Id=2";
                    Assert.Equal("two", (string)(await check.ExecuteScalarAsync())!);
                }
            });
    }

    [Fact]
    public async Task UpdateOn_With_YdbList_ChangesValues()
    {
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_update_on",
            """
            Id Int64,
            Value Text,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                await using (var seed = conn.CreateCommand())
                {
                    seed.CommandText = $"INSERT INTO {table} (Id, Value) VALUES (1,'a'),(2,'b')";
                    await seed.ExecuteNonQueryAsync();
                }

                var toUpdate = YdbList.Struct("Id", "Value")
                    .AddRow(1L, "x")
                    .AddRow(2L, "y");

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"""
                                       UPDATE {table} ON
                                       SELECT * FROM AS_TABLE($to_update);
                                       """;
                    cmd.Parameters.Add(new YdbParameter("$to_update", toUpdate));
                    await cmd.ExecuteNonQueryAsync();
                }

                await using (var check = conn.CreateCommand())
                {
                    check.CommandText = $"SELECT Value FROM {table} WHERE Id=1";
                    Assert.Equal("x", (string)(await check.ExecuteScalarAsync())!);

                    check.CommandText = $"SELECT Value FROM {table} WHERE Id=2";
                    Assert.Equal("y", (string)(await check.ExecuteScalarAsync())!);
                }
            });
    }

    [Fact]
    public async Task DeleteOn_With_YdbList_RemovesRows()
    {
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_delete_on",
            """
            Id Int64,
            Value Text,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                await using (var seed = conn.CreateCommand())
                {
                    seed.CommandText = $"INSERT INTO {table} (Id, Value) VALUES (1,'a'),(2,'b'),(3,'c')";
                    await seed.ExecuteNonQueryAsync();
                }

                var toDelete = YdbList.Struct("Id")
                    .AddRow(1L)
                    .AddRow(3L);

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"""
                                       DELETE FROM {table} ON
                                       SELECT * FROM AS_TABLE($to_delete);
                                       """;
                    cmd.Parameters.Add(new YdbParameter("$to_delete", toDelete));
                    await cmd.ExecuteNonQueryAsync();
                }

                Assert.Equal(1, await CountAsync(conn, table));

                await using (var check = conn.CreateCommand())
                {
                    check.CommandText = $"SELECT Value FROM {table} WHERE Id=2";
                    Assert.Equal("b", (string)(await check.ExecuteScalarAsync())!);
                }
            });
    }

    [Fact]
    public async Task Insert_With_OptionalText_And_Inference_NullThenNonNull()
    {
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_nulls",
            """
            Id Int64,
            Name Text?,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                var rows1 = YdbList.Struct(
                        ["Id", "Name"],
                        [YdbDbType.Int64, YdbDbType.Text])
                    .AddRow(1L, "A")
                    .AddRow(2L, null);

                await ExecAsTableAsync(conn, "INSERT", table, "$rows", rows1);

                var rows2 = YdbList.Struct(
                        ["Id", "Name"],
                        [YdbDbType.Int64, YdbDbType.Text])
                    .AddRow(3L, null)
                    .AddRow(4L, "B");

                await ExecAsTableAsync(conn, "INSERT", table, "$rows", rows2);

                await using (var check = conn.CreateCommand())
                {
                    check.CommandText = $"SELECT Name IS NULL FROM {table} WHERE Id=2";
                    Assert.True((bool)(await check.ExecuteScalarAsync())!);

                    check.CommandText = $"SELECT Name IS NULL FROM {table} WHERE Id=3";
                    Assert.True((bool)(await check.ExecuteScalarAsync())!);

                    check.CommandText = $"SELECT Name FROM {table} WHERE Id=4";
                    Assert.Equal("B", (string)(await check.ExecuteScalarAsync())!);
                }
            });
    }

    [Fact]
    public async Task Bulk_Load_With_List_Mode_Sanity()
    {
        const int n = 5_000;
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_load",
            """
            Id Int64,
            Name Text,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                for (var offset = 0; offset < n; offset += 1000)
                {
                    var rows = YdbList.Struct("Id", "Name");
                    for (var i = offset; i < Math.Min(n, offset + 1000); i++)
                        rows.AddRow((long)i, $"v{i}");

                    await ExecAsTableAsync(conn, "UPSERT", table, "$rows", rows);
                }

                Assert.Equal(n, await CountAsync(conn, table));
            });
    }

    [Fact]
    public async Task YdbList_WhenAnyRowHasNull_InsertsIntoOptionalColumn()
    {
        await using var conn = await CreateOpenConnectionAsync();
        await WithTempTableAsync(conn, "ydb_list_optional",
            """
            Id   Int64,
            Name Text?,
            PRIMARY KEY (Id)
            """,
            async table =>
            {
                var rows = YdbList.Struct("Id", "Name")
                    .AddRow(1L, "X")
                    .AddRow(2L, null);

                await ExecAsTableAsync(conn, "UPSERT", table, "$rows", rows);

                await using (var check = conn.CreateCommand())
                {
                    check.CommandText = $"SELECT Name FROM {table} WHERE Id=1";
                    Assert.Equal("X", (string)(await check.ExecuteScalarAsync())!);

                    check.CommandText = $"SELECT Name IS NULL FROM {table} WHERE Id=2";
                    Assert.True((bool)(await check.ExecuteScalarAsync())!);
                }
            });
    }
}
