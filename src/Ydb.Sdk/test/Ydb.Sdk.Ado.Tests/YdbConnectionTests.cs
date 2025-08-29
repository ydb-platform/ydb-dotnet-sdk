using System.Data;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Ado.YdbType;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests;

public sealed class YdbConnectionTests : TestBase
{
    private static readonly TemporaryTables<YdbConnectionTests> Tables = new();

    private readonly string _connectionStringTls =
        "Host=localhost;Port=2135;Database=/local;MaxSessionPool=10;RootCertificate=" +
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ca.pem");

    private volatile int _counter;


    [Fact]
    public async Task ClearPool_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var connectionString = ConnectionString + ";MaxSessionPool=100";

        var tasks = GenerateTasks(connectionString);
        tasks.Add(YdbConnection.ClearPool(new YdbConnection(connectionString)));
        tasks.AddRange(GenerateTasks(connectionString));
        await Task.WhenAll(tasks);
        Assert.Equal(999000, _counter);

        tasks = GenerateTasks(connectionString);
        tasks.Add(YdbConnection.ClearPool(new YdbConnection(connectionString)));
        await Task.WhenAll(tasks);
        Assert.Equal(1498500, _counter);
        await YdbConnection.ClearPool(new YdbConnection(connectionString));
    }

    // docker cp ydb-local:/ydb_certs/ca.pem ~/
    [Fact]
    public async Task TlsSettings_WhenUseGrpcs_ReturnValidConnection()
    {
        await using var ydbConnection = new YdbConnection(_connectionStringTls);
        await ydbConnection.OpenAsync();
        var command = ydbConnection.CreateCommand();
        command.CommandText = Tables.CreateTables;
        await command.ExecuteNonQueryAsync();
        command.CommandText = Tables.UpsertData;
        await command.ExecuteNonQueryAsync();
        command.CommandText = Tables.DeleteTables;
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Open_WhenConnectionIsOpen_ThrowException()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        Assert.Equal("Connection already open",
            Assert.Throws<InvalidOperationException>(() => ydbConnection.Open()).Message);
    }

    [Fact]
    public async Task SetConnectionString_WhenConnectionIsOpen_ThrowException()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        Assert.Equal("Connection already open",
            Assert.Throws<InvalidOperationException>(() => ydbConnection.ConnectionString = "UseTls=false").Message);
    }

    [Fact]
    public async Task BeginTransaction_WhenConnectionIsClosed_ThrowException()
    {
        var ydbConnection = await CreateOpenConnectionAsync();
        await ydbConnection.CloseAsync();
        Assert.Equal("Connection is closed",
            Assert.Throws<InvalidOperationException>(() => ydbConnection.BeginTransaction()).Message);
    }

    [Fact]
    public async Task ExecuteScalar_WhenConnectionIsClosed_ThrowException()
    {
        var ydbConnection = await CreateOpenConnectionAsync();
        await ydbConnection.CloseAsync();

        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1";

        Assert.Equal("Connection is closed",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.ExecuteScalar()).Message);
    }

    [Fact]
    public async Task ClosedYdbDataReader_WhenConnectionIsClosed_ThrowException()
    {
        var ydbConnection = await CreateOpenConnectionAsync();

        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1; SELECT 2; SELECT 3;";
        var reader = await ydbCommand.ExecuteReaderAsync();
        await reader.ReadAsync();

        Assert.Equal(1, reader.GetInt32(0));
        await ydbConnection.CloseAsync();
        Assert.Equal("The reader is closed",
            (await Assert.ThrowsAsync<InvalidOperationException>(() => reader.ReadAsync())).Message);
    }

    [Fact]
    public async Task SetNulls_WhenTableAllTypes_SussesSet() =>
        await WithAllTypesTableAsync(async (c, table) =>
        {
            var insert = c.CreateCommand();
            PrepareAllTypesInsert(insert, table);
            await insert.ExecuteNonQueryAsync();

            var select = c.CreateCommand();
            select.CommandText = $"SELECT NULL, t.* FROM {table} t";
            var r = await select.ExecuteReaderAsync();
            Assert.True(await r.ReadAsync());
            for (var i = 0; i < 21; i++) Assert.True(r.IsDBNull(i));
            Assert.False(await r.ReadAsync());
        });

    [Fact]
    public async Task DisableDiscovery_WhenPropertyIsTrue_SimpleWorking()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString += ";DisableDiscovery=true";
        await connection.OpenAsync();
        Assert.True((bool)(await new YdbCommand(connection) { CommandText = "SELECT TRUE;" }.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task OpenAsync_WhenCancelTokenIsCanceled_ThrowYdbException()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString = ConnectionString + ";MinSessionPool=1";
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await connection.OpenAsync(cts.Token));
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public async Task YdbDataReader_WhenCancelTokenIsCanceled_ThrowYdbException()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var command = new YdbCommand(connection) { CommandText = "SELECT 1; SELECT 1; SELECT 1;" };
        var ydbDataReader = await command.ExecuteReaderAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await ydbDataReader.ReadAsync(cts.Token); // first part in memory
        Assert.False(ydbDataReader.IsClosed);
        Assert.Equal(1, ydbDataReader.GetValue(0));
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Equal(StatusCode.ClientTransportTimeout,
            (await Assert.ThrowsAsync<YdbException>(async () => await ydbDataReader.NextResultAsync(cts.Token))).Code);
        Assert.True(ydbDataReader.IsClosed);
        Assert.Equal(ConnectionState.Broken, connection.State);
        // ReSharper disable once MethodSupportsCancellation
        await connection.OpenAsync();

        // ReSharper disable once MethodSupportsCancellation
        ydbDataReader = await command.ExecuteReaderAsync();
        // ReSharper disable once MethodSupportsCancellation 
        await ydbDataReader.NextResultAsync();
        await ydbDataReader.ReadAsync(cts.Token);
        Assert.False(ydbDataReader.IsClosed);
        Assert.Equal(1, ydbDataReader.GetValue(0));
        Assert.False(ydbDataReader.IsClosed);

        Assert.Equal(StatusCode.ClientTransportTimeout,
            (await Assert.ThrowsAsync<YdbException>(async () => await ydbDataReader.NextResultAsync(cts.Token))).Code);
        Assert.True(ydbDataReader.IsClosed);
        Assert.Equal(ConnectionState.Broken, connection.State);
    }

    [Fact]
    public async Task ExecuteMethods_WhenCancelTokenIsCanceled_ConnectionIsBroken()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var command = new YdbCommand(connection) { CommandText = "SELECT 1; SELECT 1; SELECT 1;" };
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await command.ExecuteReaderAsync(cts.Token));
        Assert.Equal(ConnectionState.Open, connection.State); // state is not changed

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await command.ExecuteScalarAsync(cts.Token));
        Assert.Equal(ConnectionState.Open, connection.State); // state is not changed

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await command.ExecuteNonQueryAsync(cts.Token));
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenExecutedYdbDataReaderThenCancelTokenIsCanceled_ReturnValues()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var ydbCommand = new YdbCommand(connection) { CommandText = "SELECT 1; SELECT 1; " };
        var cts = new CancellationTokenSource();
        var ydbDataReader = await ydbCommand.ExecuteReaderAsync(cts.Token);

        await ydbDataReader.ReadAsync(cts.Token);
        Assert.Equal(1, ydbDataReader.GetValue(0));
        Assert.True(await ydbDataReader.NextResultAsync(cts.Token));
        cts.Cancel();
        await ydbDataReader.ReadAsync(cts.Token);
        Assert.Equal(1, ydbDataReader.GetValue(0));
        // ReSharper disable once MethodSupportsCancellation
        Assert.False(await ydbDataReader.NextResultAsync());
    }

    private List<Task> GenerateTasks(string connectionString) => Enumerable.Range(0, 1000).Select(async i =>
    {
        YdbConnection ydbConnection;
        try
        {
            ydbConnection = CreateConnection();
            ydbConnection.ConnectionString = connectionString;
            await ydbConnection.OpenAsync();
        }
        catch (YdbException)
        {
            Interlocked.Add(ref _counter, i);
            return;
        }

        await using var connection = ydbConnection;
        var command = connection.CreateCommand();
        command.CommandText = "SELECT " + i;
        var scalar = (int)(await command.ExecuteScalarAsync())!;
        Assert.Equal(i, scalar);
        Interlocked.Add(ref _counter, scalar);
    }).ToList();

    [Fact]
    public async Task BulkUpsertImporter_HappyPath_Add_Flush() =>
        await WithIdNameTableAsync(async (c, table) =>
        {
            await ImportAsync(c, table,
                [1, "Alice"],
                [2, "Bob"]);
            Assert.Equal(2, await CountAsync(c, table));

            await ImportAsync(c, table,
                [3, "Charlie"],
                [4, "Diana"]);

            var names = await ReadNamesAsync(c, table);
            Assert.Contains("Alice", names);
            Assert.Contains("Bob", names);
            Assert.Contains("Charlie", names);
            Assert.Contains("Diana", names);
        });

    [Fact]
    public async Task BulkUpsertImporter_ThrowsOnInvalidRowCount() =>
        await WithIdNameTableAsync(async (c, table) =>
        {
            var importer = c.BeginBulkUpsertImport(table, IdNameColumns);
            await Assert.ThrowsAsync<ArgumentException>(async () => await importer.AddRowAsync(1));
            await Assert.ThrowsAsync<ArgumentException>(async () => await importer.AddRowAsync(2));
        });

    [Fact]
    public async Task BulkUpsertImporter_MultipleImporters_Parallel() =>
        await WithTwoIdNameTablesAsync(async (c, tables) =>
        {
            var t1 = tables[0];
            var t2 = tables[1];

            await Task.WhenAll(
                Task.Run(() => ImportRangeAsync(c, t1, 20, "A")),
                Task.Run(() => ImportRangeAsync(c, t2, 20, "B"))
            );

            Assert.Equal(20, await CountAsync(c, t1));
            Assert.Equal(20, await CountAsync(c, t2));
        });

    [Fact]
    public async Task BulkUpsertImporter_ThrowsOnNonexistentTable()
    {
        var tableName = $"Nonexistent_{Guid.NewGuid():N}";
        await using var conn = await CreateOpenConnectionAsync();

        var importer = conn.BeginBulkUpsertImport(tableName, IdNameColumns);

        await importer.AddRowAsync(1, "NotExists");

        await Assert.ThrowsAsync<YdbException>(async () => { await importer.FlushAsync(); });
    }

    [Fact]
    public async Task BulkUpsertImporter_AddListAsync_HappyPath_InsertsRows() =>
        await WithIdNameTableAsync((_, _) => Task.CompletedTask, idType: "Int64");

    [Fact]
    public async Task BulkUpsertImporter_AddListAsync_HappyPath_InsertsRows_Int64() =>
        await WithIdNameTableAsync(async (c, table) =>
        {
            var importer = c.BeginBulkUpsertImport(table, IdNameColumns);

            // $rows: List<Struct<Id:Int64, Name:Text>>
            var rows = YdbList
                .Struct("Id", "Name")
                .AddRow(1L, "A")
                .AddRow(2L, "B");

            await importer.AddListAsync(rows);
            await importer.FlushAsync();

            Assert.Equal(2, await CountAsync(c, table));
        }, idType: "Int64");

    [Fact]
    public async Task BulkUpsertImporter_AddListAsync_WrongStructColumns_ThrowsArgumentException() =>
        await WithIdNameTableAsync(async (c, table) =>
        {
            var importer = c.BeginBulkUpsertImport(table, IdNameColumns);

            var wrong = YdbList
                .Struct("Id", "Wrong")
                .AddRow(1L, "A");

            var ex = await Assert.ThrowsAsync<ArgumentException>(() => importer.AddListAsync(wrong).AsTask());
            Assert.Contains("mismatch", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("expected 'Name'", ex.Message, StringComparison.OrdinalIgnoreCase);
        }, idType: "Int64");

    [Fact]
    public async Task BulkUpsertImporter_AddRowAsync_WhenLaterRowHasNull_AllowsNullValue() =>
        await WithIdNameTableAsync(async (c, table) =>
        {
            var importer = c.BeginBulkUpsertImport(table, IdNameColumns);
            await importer.AddRowAsync(1, "A");
            await importer.AddRowAsync(2, new YdbParameter { YdbDbType = YdbDbType.Text, Value = null });
            await importer.FlushAsync();

            await using var check = c.CreateCommand();
            check.CommandText = $"SELECT Name FROM {table} WHERE Id=1";
            Assert.Equal("A", (string)(await check.ExecuteScalarAsync())!);

            check.CommandText = $"SELECT Name IS NULL FROM {table} WHERE Id=2";
            Assert.True((bool)(await check.ExecuteScalarAsync())!);
        }, nameNullable: true);
}
