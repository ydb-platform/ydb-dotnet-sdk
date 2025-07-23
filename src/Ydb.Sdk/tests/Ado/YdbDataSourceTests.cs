#if NET7_0_OR_GREATER
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Tests.Ado.Specification;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Ado;

public class YdbDataSourceTests : YdbAdoNetFixture
{
    private const int SelectedCount = 100;

    private readonly YdbDataSource _dataSource;

    public YdbDataSourceTests(YdbFactoryFixture fixture) : base(fixture)
    {
        _dataSource = new YdbDataSource(Fixture.ConnectionString);
    }

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
    
    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    
    [Fact]
    public async Task BulkUpsertImporter_HappyPath_Works()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database?.TrimEnd('/');

        await using var conn = await _dataSource.OpenConnectionAsync();

        using (var createCmd = conn.CreateCommand())
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

        var rows = Enumerable.Range(1, 20)
            .Select(i => new TestEntity { Id = i, Name = $"Name {i}" })
            .ToList();

        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";
        
        await using (var importer = await _dataSource.BeginBulkUpsertAsync<TestEntity>(absTablePath))
        {
            foreach (var row in rows)
                await importer.WriteRowAsync(row);
        }

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        using (var checkCmd = conn2.CreateCommand())
        {
            checkCmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
            Assert.Equal(rows.Count, count);
        }

        using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }
    
    [Fact]
    public async Task BulkUpsertImporter_InsertsNewRows()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database?.TrimEnd('/');

        await using var conn = await _dataSource.OpenConnectionAsync();

        using (var createCmd = conn.CreateCommand())
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

        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";

        var firstRows = new List<TestEntity>
        {
            new TestEntity { Id = 1, Name = "Alice" },
            new TestEntity { Id = 2, Name = "Bob" }
        };

        await using (var importer = await _dataSource.BeginBulkUpsertAsync<TestEntity>(absTablePath))
        {
            foreach (var row in firstRows)
                await importer.WriteRowAsync(row);
        }

        var newRows = new List<TestEntity>
        {
            new TestEntity { Id = 3, Name = "Charlie" },
            new TestEntity { Id = 4, Name = "Diana" }
        };

        await using (var importer = await _dataSource.BeginBulkUpsertAsync<TestEntity>(absTablePath))
        {
            foreach (var row in newRows)
                await importer.WriteRowAsync(row);
        }

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        using (var checkCmd = conn2.CreateCommand())
        {
            checkCmd.CommandText = $"SELECT Id, Name FROM {tableName}";
            var results = new Dictionary<int, string>();
            using var reader = await checkCmd.ExecuteReaderAsync();
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

        using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }
    
    [Fact]
    public async Task BulkUpsertImporter_UpdatesExistingRows()
    {
        var tableName = "BulkTest_" + Guid.NewGuid().ToString("N");
        var database = new YdbConnectionStringBuilder(_dataSource.ConnectionString).Database?.TrimEnd('/');

        await using var conn = await _dataSource.OpenConnectionAsync();

        using (var createCmd = conn.CreateCommand())
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

        var absTablePath = string.IsNullOrEmpty(database) ? tableName : $"{database}/{tableName}";

        var originalRow = new TestEntity { Id = 1, Name = "Alice" };
        await using (var importer = await _dataSource.BeginBulkUpsertAsync<TestEntity>(absTablePath))
        {
            await importer.WriteRowAsync(originalRow);
        }

        var updatedRow = new TestEntity { Id = 1, Name = "Alice Updated" };
        await using (var importer = await _dataSource.BeginBulkUpsertAsync<TestEntity>(absTablePath))
        {
            await importer.WriteRowAsync(updatedRow);
        }

        await using var conn2 = await _dataSource.OpenConnectionAsync();
        using (var selectCmd = conn2.CreateCommand())
        {
            selectCmd.CommandText = $"SELECT Name FROM {tableName} WHERE Id = 1;";
            var name = (string)await selectCmd.ExecuteScalarAsync();
            Assert.Equal("Alice Updated", name);
        }

        using (var dropCmd = conn2.CreateCommand())
        {
            dropCmd.CommandText = $"DROP TABLE {tableName}";
            await dropCmd.ExecuteNonQueryAsync();
        }
    }
}
#endif
