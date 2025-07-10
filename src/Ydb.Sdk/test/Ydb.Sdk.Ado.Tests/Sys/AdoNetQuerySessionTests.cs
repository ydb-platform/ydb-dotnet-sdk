using Xunit;

namespace Ydb.Sdk.Ado.Tests.Sys;

public class AdoNetQuerySessionTests : TestBase
{
    [Fact]
    public async Task QuerySessionPidTest()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT ClientPID FROM `.sys/query_sessions` LIMIT 1;";

        var expectedPid = Environment.ProcessId.ToString();

        await dbCommand.ExecuteNonQueryAsync();
        await using var reader = await dbCommand.ExecuteReaderAsync();

        Assert.True(reader.HasRows);
        Assert.True(await reader.ReadAsync());
        Assert.Equal(reader.GetString(0), expectedPid);
    }
}
