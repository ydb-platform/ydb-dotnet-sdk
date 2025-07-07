using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Sys;

[Trait("Category", "Integration")]
public class QueryLogTests : IClassFixture<QueryClientFixture>
{
    private readonly QueryClient _queryClient;
    
    public QueryLogTests(QueryClientFixture queryClientFixture)
    {
        _queryClient = queryClientFixture.QueryClient;
    }

    [Fact]
    public async Task QueryLogPidCheck()
    {
        const string sql = @"SELECT * FROM `.sys/query_sessions`";
        var expectedPid = Environment.ProcessId.ToString();
        
        await _queryClient.Exec(sql);
        var selectAllQueries = await _queryClient.ReadAllRows(sql);

        Assert.NotEmpty(selectAllQueries);
        Assert.Equal(selectAllQueries[0]["ClientPID"].GetOptionalUtf8(), expectedPid);
    }
}
