using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Tests.Fixture;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

[Collection("Integration QueryService test")]
[Trait("Category", "Integration")]
public class QueryIntegrationTests : IClassFixture<QueryClientFixture>, IAsyncLifetime
{
    private readonly QueryClient _queryClient;

    public QueryIntegrationTests(QueryClientFixture queryClientFixture)
    {
        _queryClient = queryClientFixture.QueryClient;
    }

    [Fact]
    public async Task Query_WhenSelectData_ReturnExpectedResult()
    {
        var selectEpisodes = await _queryClient.Query("SELECT * FROM episodes");

        Assert.Equal(70, selectEpisodes.Value.Count);

        var selectSortAndFilter = (await _queryClient.Query(@"
            SELECT series_id, season_id, episode_id, air_date, title 
            FROM episodes WHERE series_id = 1 AND season_id > 1
            ORDER BY series_id, season_id, episode_id LIMIT 3")).Value;


        Assert.Equal(3, selectSortAndFilter.Count);

        Assert.Equal("The Work Outing", selectSortAndFilter[0]["title"].GetOptionalUtf8());
        Assert.Equal("Return of the Golden Child", selectSortAndFilter[1]["title"].GetOptionalUtf8());
        Assert.Equal("Moss and the German", selectSortAndFilter[2]["title"].GetOptionalUtf8());

        var selectDataAggregation = (await _queryClient.Query(@"
            SELECT series_id, COUNT(*) AS cnt FROM episodes GROUP BY series_id;")).Value;

        Assert.Equal(2, selectDataAggregation.Count);

        Assert.Equal(24, (long)selectDataAggregation[0][1].GetUint64());
        Assert.Equal(46, (long)selectDataAggregation[1][1].GetUint64());

        var selectJoin = (await _queryClient.Query(@"
            SELECT sa.title AS season_title, sr.title AS series_title, sr.series_id, sa.season_id
            FROM seasons AS sa
            INNER JOIN series AS sr
            ON sa.series_id = sr.series_id
            WHERE sa.series_id = 1
            ORDER BY sr.series_id, sa.season_id;")).Value;

        Assert.Equal(4, selectJoin.Count);

        for (var i = 0; i < selectJoin.Count; i++)
        {
            Assert.Equal("Season " + (i + 1), selectJoin[i][0].GetOptionalUtf8());
        }
    }

    [Fact]
    public async Task QueryFetchFirstRow_UpsertDeleteSelectSingleRow_ReturnNewRow()
    {
        var status = await _queryClient.Exec(@"
            UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date) 
            VALUES ($series_id, $season_id, $episode_id, $title, $air_date)", new Dictionary<string, YdbValue>
        {
            { "$series_id", YdbValue.MakeInt64(2) },
            { "$season_id", YdbValue.MakeInt64(5) },
            { "$episode_id", YdbValue.MakeInt64(13) },
            { "$title", YdbValue.MakeUtf8("Test Episode") },
            { "$air_date", YdbValue.MakeDate(new DateTime(2018, 08, 27)) }
        });

        status.EnsureSuccess();

        var (_, row) = await _queryClient.QueryFetchFirstRow(@"
            SELECT title FROM episodes 
            WHERE series_id = $series_id AND season_id = $season_id
            AND episode_id = $episode_id;", new Dictionary<string, YdbValue>
        {
            { "$series_id", YdbValue.MakeInt64(2) },
            { "$season_id", YdbValue.MakeInt64(5) },
            { "$episode_id", YdbValue.MakeInt64(13) }
        });

        Assert.Equal("Test Episode", row![0].GetOptionalUtf8());

        status = await _queryClient.Exec("DELETE FROM episodes WHERE series_id = 2 AND season_id = 5 AND episode_id = 13");
        
        status.EnsureSuccess();

        var (selectStatus, nullRow) = await _queryClient.QueryFetchFirstRow(
            "SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5 AND episode_id = 13");
        
        selectStatus.EnsureSuccess();
        Assert.Null(nullRow);
    }

    public async Task InitializeAsync()
    {
        var status = await _queryClient.Exec(Utils.CreateTables);

        status.EnsureSuccess();

        status = await _queryClient.Exec(Utils.UpsertData);

        status.EnsureSuccess();
    }

    public async Task DisposeAsync()
    {
        var status = await _queryClient.Exec(Utils.DeleteTables);

        status.EnsureSuccess();
    }
}
