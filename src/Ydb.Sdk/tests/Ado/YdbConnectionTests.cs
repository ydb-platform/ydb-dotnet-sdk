using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
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
        Assert.Equal(999000, _counter);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection("MaxSessionPool=10")));

        await Task.WhenAll(tasks);
        Assert.Equal(1498500, _counter);
    }

    [Fact]
    public async Task ClearPoolAllPools_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        tasks.AddRange(GenerateTasks());

        await Task.WhenAll(tasks);
        Assert.Equal(999000, _counter);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        await Task.WhenAll(tasks);
        Assert.Equal(1498500, _counter);
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
    public async Task Authentication_WhenUserAndPassword_ReturnValidConnection()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = "CREATE USER kurdyukovkirya PASSWORD 'password'";
        await ydbCommand.ExecuteNonQueryAsync();

        await using var userPasswordConnection = new YdbConnection("User=kurdyukovkirya;Password=password;");
        await userPasswordConnection.OpenAsync();

        ydbCommand = userPasswordConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1 + 2";
        Assert.Equal(3, await ydbCommand.ExecuteScalarAsync());
    }

    private List<Task> GenerateTasks()
    {
        return Enumerable.Range(0, 1000).Select(async i =>
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
