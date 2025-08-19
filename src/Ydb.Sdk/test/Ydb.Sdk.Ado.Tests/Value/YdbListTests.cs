using Xunit;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Value;

public class YdbListIntegrationTests : TestBase
{
    [Fact]
    public async Task Insert_With_YdbList_Works()
    {
        var table = $"ydb_list_insert_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var create = conn.CreateCommand())
            {
                create.CommandText = $"""
                    CREATE TABLE {table} (
                        Id Int64,
                        Value Utf8,
                        PRIMARY KEY (Id)
                    )
                    """;
                await create.ExecuteNonQueryAsync();
            }

            var rows = YdbList.Struct("Id", "Value")
                .AddRow(1L, "a")
                .AddRow(2L, "b");

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"""
                    INSERT INTO {table}
                    SELECT * FROM AS_TABLE($rows);
                    """;
                cmd.Parameters.Add(new YdbParameter("$rows", rows));
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT COUNT(*) FROM {table}";
                var count = Convert.ToInt32(await check.ExecuteScalarAsync());
                Assert.Equal(2, count);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Upsert_With_YdbList_Inserts_And_Updates()
    {
        var table = $"ydb_list_upsert_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var create = conn.CreateCommand())
            {
                create.CommandText = $"""
                    CREATE TABLE {table} (
                        Id Int64,
                        Value Utf8,
                        PRIMARY KEY (Id)
                    )
                    """;
                await create.ExecuteNonQueryAsync();
            }

            await using (var seed = conn.CreateCommand())
            {
                seed.CommandText = $"INSERT INTO {table} (Id, Value) VALUES (1, 'old')";
                await seed.ExecuteNonQueryAsync();
            }

            var rows = YdbList.Struct("Id", "Value")
                .AddRow(1L, "new")
                .AddRow(2L, "two");

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"""
                    UPSERT INTO {table}
                    SELECT * FROM AS_TABLE($rows);
                    """;
                cmd.Parameters.Add(new YdbParameter("$rows", rows));
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT Value FROM {table} WHERE Id=1";
                Assert.Equal("new", (string)(await check.ExecuteScalarAsync())!);

                check.CommandText = $"SELECT Value FROM {table} WHERE Id=2";
                Assert.Equal("two", (string)(await check.ExecuteScalarAsync())!);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task UpdateOn_With_YdbList_ChangesValues()
    {
        var table = $"ydb_list_update_on_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var create = conn.CreateCommand())
            {
                create.CommandText = $"""
                    CREATE TABLE {table} (
                        Id Int64,
                        Value Utf8,
                        PRIMARY KEY (Id)
                    )
                    """;
                await create.ExecuteNonQueryAsync();
            }

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
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task DeleteOn_With_YdbList_RemovesRows()
    {
        var table = $"ydb_list_delete_on_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var create = conn.CreateCommand())
            {
                create.CommandText = $"""
                    CREATE TABLE {table} (
                        Id Int64,
                        Value Utf8,
                        PRIMARY KEY (Id)
                    )
                    """;
                await create.ExecuteNonQueryAsync();
            }

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

            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT COUNT(*) FROM {table}";
                var count = Convert.ToInt32(await check.ExecuteScalarAsync());
                Assert.Equal(1, count);

                check.CommandText = $"SELECT Value FROM {table} WHERE Id=2";
                Assert.Equal("b", (string)(await check.ExecuteScalarAsync())!);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Insert_With_OptionalUtf8_And_Inference_NullThenNonNull()
    {
        var table = $"ydb_list_nulls_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var create = conn.CreateCommand())
            {
                create.CommandText = $"""
                    CREATE TABLE {table} (
                        Id   Int64,
                        Name Utf8?,
                        PRIMARY KEY (Id)
                    )
                    """;
                await create.ExecuteNonQueryAsync();
            }

            var rows1 = YdbList.Struct(
                    ["Id", "Name"],
                    [YdbDbType.Int64, YdbDbType.Text])
                .AddRow(1L, "A")
                .AddRow(2L, null);

            await using (var insert1 = conn.CreateCommand())
            {
                insert1.CommandText = $"""
                    INSERT INTO {table}
                    SELECT * FROM AS_TABLE($rows);
                    """;
                insert1.Parameters.Add(new YdbParameter("$rows", rows1));
                await insert1.ExecuteNonQueryAsync();
            }

            var rows2 = YdbList.Struct(
                    ["Id", "Name"],
                    [YdbDbType.Int64, YdbDbType.Text])
                .AddRow(3L, null)
                .AddRow(4L, "B");

            await using (var insert2 = conn.CreateCommand())
            {
                insert2.CommandText = $"""
                    INSERT INTO {table}
                    SELECT * FROM AS_TABLE($rows);
                    """;
                insert2.Parameters.Add(new YdbParameter("$rows", rows2));
                await insert2.ExecuteNonQueryAsync();
            }

            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT Name IS NULL FROM {table} WHERE Id=2";
                Assert.True((bool)(await check.ExecuteScalarAsync())!);

                check.CommandText = $"SELECT Name IS NULL FROM {table} WHERE Id=3";
                Assert.True((bool)(await check.ExecuteScalarAsync())!);

                check.CommandText = $"SELECT Name FROM {table} WHERE Id=4";
                Assert.Equal("B", (string)(await check.ExecuteScalarAsync())!);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task Bulk_Load_With_List_Mode_Sanity()
    {
        var table = $"ydb_list_load_{Guid.NewGuid():N}";
        const int n = 5_000;

        await using var conn = await CreateOpenConnectionAsync();
        try
        {
            await using (var create = conn.CreateCommand())
            {
                create.CommandText = $"""
                    CREATE TABLE {table} (
                        Id Int64,
                        Name Utf8,
                        PRIMARY KEY (Id)
                    )
                    """;
                await create.ExecuteNonQueryAsync();
            }

            for (var offset = 0; offset < n; offset += 1000)
            {
                var rows = YdbList.Struct("Id", "Name");
                for (var i = offset; i < Math.Min(n, offset + 1000); i++)
                    rows.AddRow((long)i, $"v{i}");

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"""
                    UPSERT INTO {table}
                    SELECT * FROM AS_TABLE($rows);
                    """;
                cmd.Parameters.Add(new YdbParameter("$rows", rows));
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var check = conn.CreateCommand())
            {
                check.CommandText = $"SELECT COUNT(*) FROM {table}";
                var count = Convert.ToInt32(await check.ExecuteScalarAsync());
                Assert.Equal(n, count);
            }
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = $"DROP TABLE {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }
}
