using Xunit;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado.Tests;

public class YdbImplicitConnectionTests : TestBase
{
    [Fact]
    public async Task EnableImplicitSession_WithEnableDiscovery_Works()
    {
        await using var ydbConnection = new YdbConnection(ConnectionString
                                                          + ";EnableImplicitSession=true;DisableDiscovery=true");
        await ydbConnection.OpenAsync();
        Assert.Equal(1, await new YdbCommand(ydbConnection) { CommandText = "SELECT 1;" }.ExecuteScalarAsync());
    }
    
    [Fact]
    public async Task EnableImplicitSession_WhenFalse_AlwaysUsesPooledSession()
    {
        await using var ydbConnection = new YdbConnection(ConnectionString + ";EnableImplicitSession=false");
        await ydbConnection.OpenAsync();
        Assert.Equal(1, await new YdbCommand(ydbConnection) { CommandText = "SELECT 1;" }.ExecuteScalarAsync());
        Assert.IsNotType<ImplicitSession>(ydbConnection.Session);
    }

    [Fact]
    public async Task ImplicitSession_SimpleScalar_Works()
    {
        await using var ydbConnection = CreateConnection();
        ydbConnection.ConnectionString += ";EnableImplicitSession=true";
        await ydbConnection.OpenAsync();
        Assert.Equal(42, await new YdbCommand(ydbConnection) { CommandText = "SELECT 40 + 2;" }.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ImplicitSession_RepeatedScalars_WorksManyTimes()
    {
        await using var ydbConnection = CreateConnection();
        ydbConnection.ConnectionString += ";EnableImplicitSession=true";
        await ydbConnection.OpenAsync();

        for (var i = 0; i < 30; i++)
        {
            Assert.Equal(i, await new YdbCommand(ydbConnection) { CommandText = $"SELECT {i}" }.ExecuteScalarAsync());
        }
    }

    [Fact]
    public void ImplicitSession_ConcurrentCommand_IsStillBlockedByBusyCheck()
    {
        using var connection = CreateConnection();
        connection.ConnectionString += ";EnableImplicitSession=true";
        connection.Open();

        var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1; SELECT 1;";
        using var reader = cmd.ExecuteReader();

        var ex = Assert.Throws<YdbOperationInProgressException>(() => cmd.ExecuteReader());
        Assert.Equal("A command is already in progress: SELECT 1; SELECT 1;", ex.Message);
    }

    [Fact]
    public async Task ImplicitSession_Cancellation_AfterFirstResult_StillReturnsFirst()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString += ";EnableImplicitSession=true";
        await connection.OpenAsync();

        var cmd = new YdbCommand(connection) { CommandText = "SELECT 1; SELECT 1;" };
        using var cts = new CancellationTokenSource();

        var reader = await cmd.ExecuteReaderAsync(cts.Token);

        await reader.ReadAsync(cts.Token);
        Assert.Equal(1, reader.GetValue(0));
        Assert.True(await reader.NextResultAsync(cts.Token));

        await cts.CancelAsync();

        await reader.ReadAsync(cts.Token);
        Assert.Equal(1, reader.GetValue(0));
        Assert.False(await reader.NextResultAsync());
    }

    [Fact]
    public async Task ImplicitSession_WithExplicitTransaction_UsesExplicitSessionAndCommits()
    {
        await using var ydbConnection = CreateConnection();
        ydbConnection.ConnectionString += ";EnableImplicitSession=true";
        await ydbConnection.OpenAsync();
        await using var transaction = ydbConnection.BeginTransaction();

        Assert.Equal("Transactions are not supported in implicit session",
            (await Assert.ThrowsAsync<YdbException>(async () => await new YdbCommand(ydbConnection)
                { CommandText = "SELECT 1" }.ExecuteScalarAsync())).Message);
    }
}
