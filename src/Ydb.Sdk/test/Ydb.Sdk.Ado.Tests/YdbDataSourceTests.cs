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
        var dataSource = new YdbDataSource(ConnectionString + ";MaxSessionPool=5");
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        dataSource.Dispose();
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        dataSource.Dispose();
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
}
