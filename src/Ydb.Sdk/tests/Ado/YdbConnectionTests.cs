using System.Data;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
[Collection("YdbConnectionTests")]
public class YdbConnectionTests
{
    private static readonly TemporaryTables<YdbConnectionTests> Tables = new();

    private volatile int _counter;

    [Fact]
    public async Task ClearPool_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection("MaxSessionPool=10")));

        tasks.AddRange(GenerateTasks());

        await Task.WhenAll(tasks);
        Assert.Equal(9900, _counter);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection("MaxSessionPool=10")));

        await Task.WhenAll(tasks);
        Assert.Equal(14850, _counter);
    }

    [Fact]
    public async Task ClearPoolAllPools_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        tasks.AddRange(GenerateTasks());

        await Task.WhenAll(tasks);
        Assert.Equal(9900, _counter);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        await Task.WhenAll(tasks);
        Assert.Equal(14850, _counter);
    }

    // docker cp ydb-local:/ydb_certs/ca.pem ~/
    [Fact]
    public async Task TlsSettings_WhenUseGrpcs_ReturnValidConnection()
    {
        await using var ydbConnection = new YdbConnection(
            "Host=localhost;Port=2135;MaxSessionPool=10;RootCertificate=" +
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ca.pem"));

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
        await using var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();

        Assert.Equal("Connection already open",
            Assert.Throws<InvalidOperationException>(() => ydbConnection.Open()).Message);
    }

    [Fact]
    public async Task SetConnectionString_WhenConnectionIsOpen_ThrowException()
    {
        await using var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();

        Assert.Equal("Connection already open",
            Assert.Throws<InvalidOperationException>(() => ydbConnection.ConnectionString = "UseTls=false").Message);
    }

    [Fact]
    public async Task BeginTransaction_WhenConnectionIsClosed_ThrowException()
    {
        var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();
        await ydbConnection.CloseAsync();

        Assert.Equal("Connection is closed",
            Assert.Throws<InvalidOperationException>(() => ydbConnection.BeginTransaction()).Message);
    }

    [Fact]
    public async Task ExecuteScalar_WhenConnectionIsClosed_ThrowException()
    {
        var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();
        await ydbConnection.CloseAsync();

        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1";

        Assert.Equal("Connection is closed",
            Assert.Throws<InvalidOperationException>(() => ydbCommand.ExecuteScalar()).Message);
    }

    [Fact]
    public async Task ClosedYdbDataReader_WhenConnectionIsClosed_ThrowException()
    {
        var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();

        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1; SELECT 2; SELECT 3;";
        var reader = await ydbCommand.ExecuteReaderAsync();
        await reader.ReadAsync();

        Assert.Equal(1, reader.GetInt32(0));
        await ydbConnection.CloseAsync();
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task SetNulls_WhenTableAllTypes_SussesSet()
    {
        var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();
        var ydbCommand = ydbConnection.CreateCommand();
        var tableName = "AllTypes_" + Random.Shared.Next();

        ydbCommand.CommandText = @$"
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
";
        await ydbCommand.ExecuteNonQueryAsync();
        ydbCommand.CommandText = @$"
INSERT INTO {tableName} 
    (id, bool_column, bigint_column, smallint_column, tinyint_column, float_column, double_column, decimal_column, 
     uint8_column, uint16_column, uint32_column, uint64_column, text_column, binary_column, json_column,
     jsondocument_column, date_column, datetime_column, timestamp_column, interval_column) VALUES
($name1, $name2, $name3, $name4, $name5, $name6, $name7, $name8, $name9, $name10, $name11, $name12, $name13, $name14,
 $name14, $name15, $name16, $name17, $name18, $name19); 
";
        for (var i = 1; i < 20; i++)
        {
            ydbCommand.Parameters.AddWithValue("$name" + i, DBNull.Value);    
        }

        var ydbDataReader = await ydbCommand.ExecuteReaderAsync();
        await ydbDataReader.ReadAsync();
        
        for (var i = 0; i < 20; i++)
        {
            Assert.True(ydbDataReader.IsDBNull(i));
        }
        Assert.False(await ydbDataReader.ReadAsync());

        ydbCommand.CommandText = $"DROP TABLE {tableName}";
        await ydbCommand.ExecuteNonQueryAsync();
    }

    private List<Task> GenerateTasks()
    {
        return Enumerable.Range(0, 100).Select(async i =>
        {
            await using var connection = new YdbConnection("MaxSessionPool=10");
            await connection.OpenAsync();

            var command = connection.CreateCommand();
            command.CommandText = "SELECT " + i;

            var scalar = (int)(await command.ExecuteScalarAsync())!;
            Assert.Equal(i, scalar);

            Interlocked.Add(ref _counter, scalar);
        }).ToList();
    }
}
