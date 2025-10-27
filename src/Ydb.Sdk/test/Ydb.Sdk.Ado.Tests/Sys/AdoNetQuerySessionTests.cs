using Xunit;

namespace Ydb.Sdk.Ado.Tests.Sys;

public class AdoNetQuerySessionTests : TestBase
{
    [Fact]
    public async Task QuerySessionPidTest()
    {
        var expectedPid = Environment.ProcessId.ToString();

        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT ClientPID FROM `.sys/query_sessions` WHERE ClientPID = @pid;";
        dbCommand.Parameters.Add(new YdbParameter("pid", expectedPid));

        await dbCommand.ExecuteNonQueryAsync();
        await using var reader = await dbCommand.ExecuteReaderAsync();

        Assert.True(reader.HasRows);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(expectedPid, reader.GetString(0));
    }
}
