using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YdbImplictConnectionTests : TestBase
{
    [Fact]
    public async Task ImplicitSession_SimpleScalar_Works()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString += ";EnableImplicitSession=true";
        await connection.OpenAsync();

        var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 40 + 2;";
        var scalar = await cmd.ExecuteScalarAsync();
        Assert.Equal(42, Convert.ToInt32(scalar));
    }

    [Fact]
    public async Task ImplicitSession_RepeatedScalars_WorksManyTimes()
    {
        await using var connection = CreateConnection();
        connection.ConnectionString += ";EnableImplicitSession=true";
        await connection.OpenAsync();

        for (var i = 0; i < 30; i++)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT {i};";
            var scalar = await cmd.ExecuteScalarAsync();
            Assert.Equal(i, Convert.ToInt32(scalar));
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
}
