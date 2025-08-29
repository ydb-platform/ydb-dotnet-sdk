using System.Data;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests;

public abstract class TestBase : IAsyncLifetime
{
    protected static string ConnectionString => TestUtils.ConnectionString;

    protected static readonly string[] IdNameColumns = ["Id", "Name"];

    protected static YdbConnection CreateConnection() => new(
        new YdbConnectionStringBuilder(ConnectionString) { LoggerFactory = TestUtils.LoggerFactory }
    );

    protected static YdbConnection CreateOpenConnection()
    {
        var connection = CreateConnection();
        connection.Open();
        return connection;
    }

    protected static async Task<YdbConnection> CreateOpenConnectionAsync()
    {
        var connection = CreateConnection();
        await connection.OpenAsync();
        return connection;
    }

    private static string CreateIdNameTableSql(string table, string idType = "Int32", bool nameNullable = false) => $"""
         CREATE TABLE {table} (
                 Id {idType},
                 Name Text{(nameNullable ? "?" : "")},
             PRIMARY KEY (Id)
         )
         """;

    private static string CreateAllTypesTableSql(string table) => @$"
CREATE TABLE {table} (
    id INT32,
    bool_column BOOL,
    bigint_column INT64,
    smallint_column INT16,
    tinyint_column INT8,
    float_column FLOAT,
    double_column DOUBLE,
    decimal_column DECIMAL(22,9),
    uint8_column UINT8,
    uint16_column UINT16,
    uint32_column UINT32,
    uint64_column UINT64,
    text_column TEXT,
    binary_column BYTES,
    json_column JSON,
    jsondocument_column JSONDOCUMENT,
    date_column DATE,
    datetime_column DATETIME,
    timestamp_column TIMESTAMP,
    interval_column INTERVAL,
    PRIMARY KEY (id)
)
";

    private static async Task UsingTempTableCoreAsync(
        YdbConnection conn,
        Func<string, string> createSqlFactory,
        Func<YdbConnection, string, Task> body,
        Func<string, string>? dropSqlFactory = null)
    {
        var table = $"tmp_{Guid.NewGuid():N}";

        await using (var create = conn.CreateCommand())
        {
            create.CommandText = createSqlFactory(table);
            await create.ExecuteNonQueryAsync();
        }

        try
        {
            await body(conn, table);
        }
        finally
        {
            await using var drop = conn.CreateCommand();
            drop.CommandText = (dropSqlFactory ?? (t => $"DROP TABLE {t}"))(table);
            try
            {
                await drop.ExecuteNonQueryAsync();
            }
            catch
            {
                /* ignore */
            }
        }
    }

    private static async Task UsingTempTableAsync(
        Func<string, string> createSqlFactory,
        Func<YdbConnection, string, Task> body,
        Func<string, string>? dropSqlFactory = null)
    {
        await using var conn = await CreateOpenConnectionAsync();
        await UsingTempTableCoreAsync(conn, createSqlFactory, body, dropSqlFactory);
    }

    protected static Task WithIdNameTableAsync(
        Func<YdbConnection, string, Task> body,
        string idType = "Int32",
        bool nameNullable = false) =>
        UsingTempTableAsync(t => CreateIdNameTableSql(t, idType, nameNullable), body);

    protected static Task WithAllTypesTableAsync(Func<YdbConnection, string, Task> body) =>
        UsingTempTableAsync(CreateAllTypesTableSql, body);

    private static async Task UsingTempTablesCoreAsync(
        YdbConnection conn,
        int count,
        Func<string[], string[]> createSqlsFactory,
        Func<YdbConnection, string[], Task> body,
        Func<string[], string[]>? dropSqlsFactory = null)
    {
        var names = Enumerable.Range(0, count).Select(_ => $"tmp_{Guid.NewGuid():N}").ToArray();

        foreach (var sql in createSqlsFactory(names))
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
        }

        try
        {
            await body(conn, names);
        }
        finally
        {
            var drops = (dropSqlsFactory ?? (ts => ts.Select(t => $"DROP TABLE {t}").ToArray()))(names);
            foreach (var sql in drops)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                try
                {
                    await cmd.ExecuteNonQueryAsync();
                }
                catch
                {
                    /* ignore */
                }
            }
        }
    }

    private static async Task UsingTempTablesAsync(
        int count,
        Func<string[], string[]> createSqlsFactory,
        Func<YdbConnection, string[], Task> body,
        Func<string[], string[]>? dropSqlsFactory = null)
    {
        await using var conn = await CreateOpenConnectionAsync();
        await UsingTempTablesCoreAsync(conn, count, createSqlsFactory, body, dropSqlsFactory);
    }

    protected static Task WithTwoIdNameTablesAsync(
        Func<YdbConnection, string[], Task> body,
        string idType = "Int32",
        bool nameNullable = false) =>
        UsingTempTablesAsync(
            2,
            tables => tables.Select(t => CreateIdNameTableSql(t, idType, nameNullable)).ToArray(),
            body
        );

    protected static async Task<int> CountAsync(YdbConnection c, string table)
    {
        await using var cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    protected static async Task<List<string>> ReadNamesAsync(YdbConnection c, string table)
    {
        var names = new List<string>();
        await using var cmd = c.CreateCommand();
        cmd.CommandText = $"SELECT Name FROM {table} ORDER BY Id";
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) names.Add(r.GetString(0));
        return names;
    }

    protected static async Task ImportAsync(YdbConnection c, string table, params object[][] rows)
    {
        var importer = c.BeginBulkUpsertImport(table, IdNameColumns);
        foreach (var row in rows) await importer.AddRowAsync(row);
        await importer.FlushAsync();
    }

    protected static async Task ImportRangeAsync(YdbConnection c, string table, int n, string prefix)
    {
        var importer = c.BeginBulkUpsertImport(table, IdNameColumns);
        foreach (var row in Enumerable.Range(0, n).Select(i => new object[] { i, $"{prefix}{i}" }))
            await importer.AddRowAsync(row);
        await importer.FlushAsync();
    }

    protected static void PrepareAllTypesInsert(YdbCommand cmd, string table)
    {
        cmd.CommandText = @$"
INSERT INTO {table} 
    (id, bool_column, bigint_column, smallint_column, tinyint_column, float_column, double_column, decimal_column, 
     uint8_column, uint16_column, uint32_column, uint64_column, text_column, binary_column, json_column,
     jsondocument_column, date_column, datetime_column, timestamp_column, interval_column) VALUES
(@name1, @name2, @name3, @name4, @name5, @name6, @name7, @name8, @name9, @name10, @name11, @name12, @name13, @name14,
 @name15, @name16, @name17, @name18, @name19, @name20); 
";
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name1", DbType = DbType.Int32, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name2", DbType = DbType.Boolean, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name3", DbType = DbType.Int64, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name4", DbType = DbType.Int16, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name5", DbType = DbType.SByte, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name6", DbType = DbType.Single, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name7", DbType = DbType.Double, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name8", DbType = DbType.Decimal, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name9", DbType = DbType.Byte, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name10", DbType = DbType.UInt16, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name11", DbType = DbType.UInt32, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name12", DbType = DbType.UInt64, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name13", DbType = DbType.String, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name14", DbType = DbType.Binary, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name15", YdbDbType = YdbDbType.Json });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name16", YdbDbType = YdbDbType.JsonDocument });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name17", DbType = DbType.Date, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name18", DbType = DbType.DateTime, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name19", DbType = DbType.DateTime2, Value = null });
        cmd.Parameters.Add(new YdbParameter { ParameterName = "name20", YdbDbType = YdbDbType.Interval });
    }

    public async Task InitializeAsync() => await OnInitializeAsync().ConfigureAwait(false);

    public async Task DisposeAsync() => await OnDisposeAsync().ConfigureAwait(false);

    protected virtual Task OnInitializeAsync() => Task.CompletedTask;

    protected virtual Task OnDisposeAsync() => Task.CompletedTask;
}
