#if NET7_0_OR_GREATER
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Tests.Ado.Specification;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Ado;

[Collection("YdbDataSourceTests")]
[CollectionDefinition("YdbDataSourceTests isolation test", DisableParallelization = true)]
public class YdbDataSourceTests : YdbAdoNetFixture
{
    private const int SelectedCount = 100;

    private readonly YdbDataSource _dataSource;

    public YdbDataSourceTests(YdbFactoryFixture fixture) : base(fixture)
    {
        _dataSource = new YdbDataSource($"{Fixture.ConnectionString};MaxSessionPool=10");
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
            Assert.Equal(1, _dataSource.CreateCommand("SELECT 1;").ExecuteScalar());
        }

        _dataSource.Dispose();
        for (var i = 0; i < SelectedCount; i++)
        {
            Assert.Equal(1, _dataSource.CreateCommand("SELECT 1;").ExecuteScalar());
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
}
#endif
