using System.Data;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Ado.YdbType;

namespace Ydb.Sdk.Ado.Tests;

public sealed class YdbConnectionTests : TestBase
{
    private static readonly TemporaryTables<YdbConnectionTests> Tables = new();

    private readonly string _connectionStringTls =
        "Host=localhost;Port=2135;Database=/local;MaxPoolSize=10;RootCertificate=" +
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ca.pem");

    private volatile int _counter;


    [Fact]
    public async Task ClearPool_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var connectionString = ConnectionString + ";MaxPoolSize=100";

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
    public async Task SetNulls_WhenTableAllTypes_SussesSet()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ydbCommand = ydbConnection.CreateCommand();
        var tableName = "AllTypes_" + Random.Shared.Next();

        ydbCommand.CommandText = $"""
                                  CREATE TABLE {tableName} (
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
                                  """;
        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText =
            $"""
             INSERT INTO {tableName} (id, bool_column, bigint_column, smallint_column, tinyint_column, float_column,
             double_column, decimal_column, uint8_column, uint16_column, uint32_column, uint64_column, text_column, 
             binary_column, json_column, jsondocument_column, date_column, datetime_column, timestamp_column, 
             interval_column) VALUES (@name1, @name2, @name3, @name4, @name5, @name6, @name7, @name8, @name9, @name10, 
             @name11, @name12, @name13, @name14, @name15, @name16, @name17, @name18, @name19, @name20); 
             """;

        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name1", DbType = DbType.Int32, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name2", DbType = DbType.Boolean, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name3", DbType = DbType.Int64, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name4", DbType = DbType.Int16, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name5", DbType = DbType.SByte, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name6", DbType = DbType.Single, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name7", DbType = DbType.Double, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name8", DbType = DbType.Decimal, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name9", DbType = DbType.Byte, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name10", DbType = DbType.UInt16, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name11", DbType = DbType.UInt32, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name12", DbType = DbType.UInt64, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name13", DbType = DbType.String, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name14", DbType = DbType.Binary, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name15", YdbDbType = YdbDbType.Json });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name16", YdbDbType = YdbDbType.JsonDocument });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name17", DbType = DbType.Date, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter
            { ParameterName = "name18", DbType = DbType.DateTime, Value = null });
        ydbCommand.Parameters.Add(
            new YdbParameter { ParameterName = "name19", DbType = DbType.DateTime2, Value = null });
        ydbCommand.Parameters.Add(new YdbParameter { ParameterName = "name20", YdbDbType = YdbDbType.Interval });

        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText = $"SELECT NULL, t.* FROM {tableName} t";
        var ydbDataReader = await ydbCommand.ExecuteReaderAsync();
        Assert.True(await ydbDataReader.ReadAsync());
        for (var i = 0; i < 21; i++)
        {
            Assert.True(ydbDataReader.IsDBNull(i));
        }

        Assert.False(await ydbDataReader.ReadAsync());

        ydbCommand.CommandText = $"DROP TABLE {tableName}";
        await ydbCommand.ExecuteNonQueryAsync();
    }

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
        connection.ConnectionString = ConnectionString + ";MinPoolSize=1";
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
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
        await cts.CancelAsync();

        await ydbDataReader.ReadAsync(cts.Token); // first part in memory
        Assert.False(ydbDataReader.IsClosed);
        Assert.Equal(1, ydbDataReader.GetValue(0));
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Equal(StatusCode.ClientTransportTimeout,
            (await Assert.ThrowsAsync<YdbException>(async () => await ydbDataReader.NextResultAsync(cts.Token))).Code);
        Assert.True(ydbDataReader.IsClosed);
        Assert.Equal(ConnectionState.Broken, connection.State);
        // CLOSE OLD CONNECTION! (return to pool)
        await connection.CloseAsync();
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
        await cts.CancelAsync();
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
        catch (ObjectDisposedException)
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
    public async Task BulkUpsertImporter_HappyPath_Add_Flush()
    {
        var tableName = $"BulkImporter_{Guid.NewGuid():N}";

        await using var ydbConnection = await CreateOpenConnectionAsync();
        try
        {
            await using (var createCmd = ydbConnection.CreateCommand())
            {
                createCmd.CommandText = $"""
                                         CREATE TABLE {tableName} (
                                             Id Int32,
                                             Name Text,
                                             PRIMARY KEY (Id)
                                         )
                                         """;
                await createCmd.ExecuteNonQueryAsync();
            }

            var columns = new[] { "Id", "Name" };

            var importer = ydbConnection.BeginBulkUpsertImport(tableName, columns);

            await importer.AddRowAsync(1, "Alice");
            await importer.AddRowAsync(2, "Bob");
            await importer.FlushAsync();

            await using (var checkCmd = ydbConnection.CreateCommand())
            {
                checkCmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
                var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
                Assert.Equal(2, count);
            }

            importer = ydbConnection.BeginBulkUpsertImport(tableName, columns);
            await importer.AddRowAsync(3, "Charlie");
            await importer.AddRowAsync(4, "Diana");
            await importer.FlushAsync();

            await using (var checkCmd = ydbConnection.CreateCommand())
            {
                checkCmd.CommandText = $"SELECT Name FROM {tableName} ORDER BY Id";
                var names = new List<string>();
                await using var reader = await checkCmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    names.Add(reader.GetString(0));
                Assert.Contains("Alice", names);
                Assert.Contains("Bob", names);
                Assert.Contains("Charlie", names);
                Assert.Contains("Diana", names);
            }
        }
        finally
        {
            await using var dropCmd = ydbConnection.CreateCommand();
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task BulkUpsertImporter_ThrowsOnInvalidRowCount()
    {
        var tableName = $"BulkImporter_{Guid.NewGuid():N}";
        await using var ydbConnection = await CreateOpenConnectionAsync();
        try
        {
            await using (var createCmd = ydbConnection.CreateCommand())
            {
                createCmd.CommandText = $"CREATE TABLE {tableName} (Id Int32, Name Utf8, PRIMARY KEY (Id))";
                await createCmd.ExecuteNonQueryAsync();
            }

            var columns = new[] { "Id", "Name" };

            var importer = ydbConnection.BeginBulkUpsertImport(tableName, columns);

            await Assert.ThrowsAsync<ArgumentException>(async () => await importer.AddRowAsync(1));
            await Assert.ThrowsAsync<ArgumentException>(async () => await importer.AddRowAsync(2));
        }
        finally
        {
            await using var dropCmd = ydbConnection.CreateCommand();
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task BulkUpsertImporter_MultipleImporters_Parallel()
    {
        var table1 = $"BulkImporter_{Guid.NewGuid():N}_1";
        var table2 = $"BulkImporter_{Guid.NewGuid():N}_2";

        await using var ydbConnection = await CreateOpenConnectionAsync();
        try
        {
            foreach (var table in new[] { table1, table2 })
            {
                await using var createCmd = ydbConnection.CreateCommand();
                createCmd.CommandText = $"""
                                         CREATE TABLE {table} (
                                             Id Int32,
                                             Name Utf8,
                                             PRIMARY KEY (Id)
                                         )
                                         """;
                await createCmd.ExecuteNonQueryAsync();
            }

            var columns = new[] { "Id", "Name" };

            await Task.WhenAll(
                Task.Run(async () =>
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var importer = ydbConnection.BeginBulkUpsertImport(table1, columns);
                    var rows = Enumerable.Range(0, 20)
                        .Select(i => new object[] { i, $"A{i}" })
                        .ToArray();
                    foreach (var row in rows)
                        await importer.AddRowAsync(row);
                    await importer.FlushAsync();
                }),
                Task.Run(async () =>
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var importer = ydbConnection.BeginBulkUpsertImport(table2, columns);
                    var rows = Enumerable.Range(0, 20)
                        .Select(i => new object[] { i, $"B{i}" })
                        .ToArray();
                    foreach (var row in rows)
                        await importer.AddRowAsync(row);
                    await importer.FlushAsync();
                })
            );

            foreach (var table in new[] { table1, table2 })
            {
                await using var checkCmd = ydbConnection.CreateCommand();
                checkCmd.CommandText = $"SELECT COUNT(*) FROM {table}";
                var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
                Assert.Equal(20, count);
            }
        }
        finally
        {
            foreach (var table in new[] { table1, table2 })
            {
                await using var dropCmd = ydbConnection.CreateCommand();
                dropCmd.CommandText = $"DROP TABLE {table}";
                await dropCmd.ExecuteNonQueryAsync();
            }
        }
    }

    [Fact]
    public async Task BulkUpsertImporter_ThrowsOnNonexistentTable()
    {
        var tableName = $"Nonexistent_{Guid.NewGuid():N}";
        await using var ydbConnection = await CreateOpenConnectionAsync();

        var columns = new[] { "Id", "Name" };

        var importer = ydbConnection.BeginBulkUpsertImport(tableName, columns);

        await importer.AddRowAsync(1, "NotExists");

        await Assert.ThrowsAsync<YdbException>(async () => { await importer.FlushAsync(); });
    }

    [Fact]
    public async Task ClearPool_FireAndForget_DoesNotBlock_And_PoolsRecreate()
    {
        var csPooled = ConnectionString +
                       ";UseTls=false;DisableDiscovery=true" +
                       ";CreateSessionTimeout=3;ConnectTimeout=3" +
                       ";KeepAlivePingDelay=0;KeepAlivePingTimeout=0";
        var csImplicit = csPooled + ";EnableImplicitSession=true";

        await using (var warmPooled = new YdbConnection(csPooled))
        {
            await warmPooled.OpenAsync();
            await using var cmd = warmPooled.CreateCommand();
            cmd.CommandText = "SELECT 1";
            Assert.Equal(1L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }

        await using (var warmImplicit = new YdbConnection(csImplicit))
        {
            await warmImplicit.OpenAsync();
            await using var cmd = warmImplicit.CreateCommand();
            cmd.CommandText = "SELECT 1";
            Assert.Equal(1L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }

        var clearPooledTask = YdbConnection.ClearPool(new YdbConnection(csPooled));
        var clearImplicitTask = YdbConnection.ClearPool(new YdbConnection(csImplicit));

        await Task.WhenAll(clearPooledTask, clearImplicitTask);

        await using (var checkPooled = new YdbConnection(csPooled))
        {
            await checkPooled.OpenAsync();
            await using var cmd = checkPooled.CreateCommand();
            cmd.CommandText = "SELECT 1";
            Assert.Equal(1L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }

        await using (var checkImplicit = new YdbConnection(csImplicit))
        {
            await checkImplicit.OpenAsync();
            await using var cmd = checkImplicit.CreateCommand();
            cmd.CommandText = "SELECT 1";
            Assert.Equal(1L, Convert.ToInt64(await cmd.ExecuteScalarAsync()));
        }
    }
}
