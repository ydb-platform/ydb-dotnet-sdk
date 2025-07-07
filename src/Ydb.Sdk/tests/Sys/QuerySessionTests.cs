using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Tests.Ado.Specification;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Sys;

public class QuerySessionTests : YdbAdoNetFixture
{
    public QuerySessionTests(YdbFactoryFixture fixture) : base(fixture)
    {
    }

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
