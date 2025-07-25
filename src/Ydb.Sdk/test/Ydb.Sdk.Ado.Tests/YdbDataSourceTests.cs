using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YdbDataSourceTests : TestBase
{
    private const int SelectedCount = 100;

    private readonly YdbDataSource _dataSource = new(ConnectionString);

    [Fact]
    public async Task OpenConnectionAsync_WhenMaxSessionPool10_ReturnOpenConnection()
    {
        var tasks = new Task[SelectedCount];
        for (var i = 0; i < SelectedCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                await using var ydbConnection = await _dataSource.OpenConnectionAsync();
                var ydbCommand = new YdbCommand(ydbConnection) { CommandText = "SELECT 1" };
                Assert.Equal(1, await ydbCommand.ExecuteScalarAsync());
            });
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public void CreateCommand_FromDataSource_ReturnDbCommand()
    {
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = _dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        _dataSource.Dispose();
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = _dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }
    }

    [Fact]
    public void CreateConnection_FromDataSource_ReturnNotOpenConnection()
    {
        using var ydbConnection = _dataSource.CreateConnection();
        ydbConnection.Open();
        Assert.Equal(1, new YdbCommand(ydbConnection) { CommandText = "SELECT 1" }.ExecuteScalar());
    }

    [Fact]
    public void OpenConnection_FromDataSource_ReturnOpenConnection()
    {
        using var ydbConnection = _dataSource.OpenConnection();
        Assert.Equal(1, new YdbCommand(ydbConnection) { CommandText = "SELECT 1" }.ExecuteScalar());
    }

    [Fact]
    public async Task BulkUpsert_HappyPath_DS()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database.TrimEnd('/');
        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using (var createCmd = conn.CreateCommand())
        {
            createCmd.CommandText = $@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Utf8,
                PRIMARY KEY (Id)
            )";
            await createCmd.ExecuteNonQueryAsync();
        }

        await conn.CloseAsync();

        var columns = new[] { "Id", "Name" };
        var rows = Enumerable.Range(1, 20)
            .Select(i => new object?[] { i, $"Name {i}" })
            .ToList();

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        await conn2.BulkUpsertAsync(absTablePath, columns, rows);

        await using (var checkCmd = conn2.CreateCommand())
        {
            checkCmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
            Assert.Equal(rows.Count, count);
        }

        await using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task BulkUpsert_InsertsNewRows_DS()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database.TrimEnd('/');
        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using (var createCmd = conn.CreateCommand())
        {
            createCmd.CommandText = $@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Utf8,
                PRIMARY KEY (Id)
            )";
            await createCmd.ExecuteNonQueryAsync();
        }

        await conn.CloseAsync();

        var columns = new[] { "Id", "Name" };
        var firstRows = new List<object[]>
        {
            new object[] { 1, "Alice" },
            new object[] { 2, "Bob" }
        };

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        await conn2.BulkUpsertAsync(absTablePath, columns, firstRows);

        var newRows = new List<object[]>
        {
            new object[] { 3, "Charlie" },
            new object[] { 4, "Diana" }
        };
        await conn2.BulkUpsertAsync(absTablePath, columns, newRows);

        await using (var checkCmd = conn2.CreateCommand())
        {
            checkCmd.CommandText = $"SELECT Id, Name FROM {tableName}";
            var results = new Dictionary<int, string>();
            await using var reader = await checkCmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                results[reader.GetInt32(0)] = reader.GetString(1);
            }

            Assert.Equal(4, results.Count);
            Assert.Equal("Alice", results[1]);
            Assert.Equal("Bob", results[2]);
            Assert.Equal("Charlie", results[3]);
            Assert.Equal("Diana", results[4]);
        }

        await using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task BulkUpsert_UpdatesExistingRows_DS()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database.TrimEnd('/');
        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using (var createCmd = conn.CreateCommand())
        {
            createCmd.CommandText = $@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Utf8,
                PRIMARY KEY (Id)
            )";
            await createCmd.ExecuteNonQueryAsync();
        }

        await conn.CloseAsync();

        var columns = new[] { "Id", "Name" };
        var originalRow = new object?[] { 1, "Alice" };

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        await conn2.BulkUpsertAsync(absTablePath, columns, [originalRow]);

        var updatedRow = new object?[] { 1, "Alice Updated" };
        await conn2.BulkUpsertAsync(absTablePath, columns, [updatedRow]);

        await using (var selectCmd = conn2.CreateCommand())
        {
            selectCmd.CommandText = $"SELECT Name FROM {tableName} WHERE Id = 1;";
            var name = await selectCmd.ExecuteScalarAsync() as string;
            Assert.Equal("Alice Updated", name);
        }

        await using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task BulkUpsert_DirectlyOnDataSource_Works()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database.TrimEnd('/');
        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";

        await using var conn = await _dataSource.OpenConnectionAsync();
        await using (var createCmd = conn.CreateCommand())
        {
            createCmd.CommandText = $@"
            CREATE TABLE {tableName} (
                Id Int32,
                Name Utf8,
                PRIMARY KEY (Id)
            )";
            await createCmd.ExecuteNonQueryAsync();
        }

        await conn.CloseAsync();

        var columns = new[] { "Id", "Name" };
        var rows = Enumerable.Range(1, 5)
            .Select(i => new object?[] { i, $"Name {i}" })
            .ToList();

        await _dataSource.BulkUpsertAsync(absTablePath, columns, rows);

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        await using (var checkCmd = conn2.CreateCommand())
        {
            checkCmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
            Assert.Equal(rows.Count, count);
        }

        await using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }
}
