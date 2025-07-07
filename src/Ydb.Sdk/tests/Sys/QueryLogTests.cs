using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Sys;

public class QueryLogTests : IClassFixture<QueryClientFixture>
{
    private readonly QueryClient _queryClient;
    
    public QueryLogTests(QueryClientFixture queryClientFixture)
    {
        _queryClient = queryClientFixture.QueryClient;
    }

    [Fact]
    public async Task QuerySessionPidTest()
    {
        const string sql = @"SELECT * FROM `.sys/query_sessions` LIMIT 1";
        var expectedPid = Environment.ProcessId.ToString();
        
        await _queryClient.Exec(sql);
        var sessionRow = await _queryClient.ReadRow(sql);

        Assert.NotNull(sessionRow);
        Assert.Equal(sessionRow["ClientPID"].GetOptionalUtf8(), expectedPid);
    }
}
