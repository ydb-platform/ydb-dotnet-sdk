using Xunit;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Tests.Fixture;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

[Collection("Integration QueryService test")]
[Trait("Category", "Integration")]
public class QueryIntegrationTests : IClassFixture<QueryClientFixture>, IAsyncLifetime
{
    private static readonly TemporaryTables<QueryIntegrationTests> Tables = new();

    private readonly QueryClient _queryClient;

    public QueryIntegrationTests(QueryClientFixture queryClientFixture)
    {
        _queryClient = queryClientFixture.QueryClient;
    }

    [Fact]
    public async Task ReadAllRows_WhenSelectData_ReturnExpectedResult()
    {
        var selectEpisodes = await _queryClient.ReadAllRows($"SELECT * FROM {Tables.Episodes}");

        Assert.Equal(70, selectEpisodes.Count);

        var selectSortAndFilter = await _queryClient.ReadAllRows($@"
            SELECT series_id, season_id, episode_id, air_date, title 
            FROM {Tables.Episodes} WHERE series_id = 1 AND season_id > 1
            ORDER BY series_id, season_id, episode_id LIMIT 3");

        Assert.Equal(3, selectSortAndFilter.Count);

        Assert.Equal("The Work Outing", selectSortAndFilter[0]["title"].GetOptionalUtf8());
        Assert.Equal("Return of the Golden Child", selectSortAndFilter[1]["title"].GetOptionalUtf8());
        Assert.Equal("Moss and the German", selectSortAndFilter[2]["title"].GetOptionalUtf8());

        var selectDataAggregation = await _queryClient.ReadAllRows($@"
            SELECT series_id, COUNT(*) AS cnt FROM {Tables.Episodes} GROUP BY series_id ORDER BY series_id;");

        Assert.Equal(2, selectDataAggregation.Count);

        Assert.Equal(24, (long)selectDataAggregation[0][1].GetUint64());
        Assert.Equal(46, (long)selectDataAggregation[1][1].GetUint64());

        var selectJoin = await _queryClient.ReadAllRows($@"
            SELECT sa.title AS season_title, sr.title AS series_title, sr.series_id, sa.season_id
            FROM {Tables.Seasons} AS sa
            INNER JOIN {Tables.Series} AS sr
            ON sa.series_id = sr.series_id
            WHERE sa.series_id = 1
            ORDER BY sr.series_id, sa.season_id;");

        Assert.Equal(4, selectJoin.Count);

        for (var i = 0; i < selectJoin.Count; i++)
        {
            Assert.Equal("Season " + (i + 1), selectJoin[i][0].GetOptionalUtf8());
        }
    }

    [Fact]
    public async Task ReadRows_UpsertDeleteSelectSingleRow_ReturnNewRow()
    {
        await _queryClient.Exec($@"
            UPSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date) 
            VALUES ($series_id, $season_id, $episode_id, $title, $air_date)", new Dictionary<string, YdbValue>
        {
            { "$series_id", YdbValue.MakeUint64(2) },
            { "$season_id", YdbValue.MakeUint64(5) },
            { "$episode_id", YdbValue.MakeUint64(13) },
            { "$title", YdbValue.MakeUtf8("Test Episode") },
            { "$air_date", YdbValue.MakeDate(new DateTime(2018, 08, 27)) }
        });

        var row = await _queryClient.ReadRow($@"
            SELECT title FROM {Tables.Episodes} 
            WHERE series_id = $series_id AND season_id = $season_id AND episode_id = $episode_id;",
            new Dictionary<string, YdbValue>
            {
                { "$series_id", YdbValue.MakeUint64(2) },
                { "$season_id", YdbValue.MakeUint64(5) },
                { "$episode_id", YdbValue.MakeUint64(13) }
            });

        Assert.Equal("Test Episode", row![0].GetOptionalUtf8());

        await _queryClient.Exec(
            $"DELETE FROM {Tables.Episodes} WHERE series_id = 2 AND season_id = 5 AND episode_id = 13");

        var nullRow = await _queryClient.ReadRow(
            $"SELECT * FROM {Tables.Episodes} WHERE series_id = 2 AND season_id = 5 AND episode_id = 13");

        Assert.Null(nullRow);
    }

    [Fact]
    public async Task DoTx_UpsertThenRollbackTransaction_ReturnOldRow()
    {
        await _queryClient.DoTx(async queryTx =>
        {
            await queryTx.Exec($@"
                    UPSERT INTO {Tables.Seasons} (series_id, season_id, first_aired) VALUES
                    ($series_id, $season_id, $air_date);
                ", new Dictionary<string, YdbValue>
            {
                { "$series_id", YdbValue.MakeUint64(1) },
                { "$season_id", YdbValue.MakeUint64(3) },
                { "$air_date", YdbValue.MakeDate(new DateTime(2022, 2, 24)) }
            });

            await queryTx.Rollback();
        });

        var row = await _queryClient.ReadRow(
            $"SELECT first_aired FROM {Tables.Seasons} WHERE series_id = 1 AND season_id = 3");

        Assert.Equal(new DateTime(2008, 11, 21), row![0].GetOptionalDate());
    }

    [Fact]
    public async Task DoTx_UpsertThenExceptionInTransaction_ReturnOldRow()
    {
        try
        {
            await _queryClient.DoTx(async queryTx =>
            {
                await queryTx.Exec($@"
                    UPSERT INTO {Tables.Seasons} (series_id, season_id, first_aired) VALUES
                    ($series_id, $season_id, $air_date);
                ", new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) },
                    { "$air_date", YdbValue.MakeDate(new DateTime(2022, 2, 24)) }
                });

                throw new Exception();
            });
        }
        catch (Exception)
        {
            // ignored
        }

        var row = await _queryClient.ReadRow(
            $"SELECT first_aired FROM {Tables.Seasons} WHERE series_id = 1 AND season_id = 3");

        Assert.Equal(new DateTime(2008, 11, 21), row![0].GetOptionalDate());
    }

    [Fact]
    public async Task DoTx_InteractiveTransactionInAndOutCommitOperation_UpsertNewValue()
    {
        await _queryClient.DoTx(async tx =>
        {
            await tx.Exec($@"
                INSERT INTO {Tables.Seasons} (series_id, season_id, title, first_aired, last_aired) 
                VALUES (2, 6, ""Season6"", Date(""2006-02-03""), Date(""2006-03-03""))");

            await tx.Exec($@"
                INSERT INTO {Tables.Episodes} (series_id, season_id, episode_id, title, air_date)
                VALUES (2, 6, 1, ""Yesterday's Jam"", Date(""2006-02-03""))");
        }); // commit

        var titles = await _queryClient.DoTx<string>(
            async tx =>
            {
                var r1 = await tx.ReadRow($"SELECT title FROM {Tables.Seasons} WHERE series_id = 2 AND season_id = 6");
                var r2 = await tx.ReadRow($"SELECT title FROM {Tables.Episodes} " +
                                          "WHERE series_id = 2 AND season_id = 6 AND episode_id = 1");

                return r1![0].GetOptionalUtf8() + "_" + r2![0].GetOptionalUtf8();
            }, TxMode.SnapshotRo);

        Assert.Equal("Season6_Yesterday's Jam", titles);

        await _queryClient.DoTx(async tx =>
        {
            await tx.Exec($"DELETE FROM {Tables.Seasons} WHERE series_id = 2 AND season_id = 6");

            await tx.Exec($"DELETE FROM {Tables.Episodes} WHERE series_id = 2 AND season_id = 6 AND episode_id = 1",
                commit: true); // commit with operation
        });

        var row1 = await _queryClient.ReadRow(
            $"SELECT title FROM {Tables.Seasons} WHERE series_id = 2 AND season_id = 6",
            txMode: TxMode.SnapshotRo);
        var row2 = await _queryClient.ReadRow(
            $"SELECT title FROM {Tables.Episodes} WHERE series_id = 2 AND season_id = 6 AND episode_id = 1",
            txMode: TxMode.SnapshotRo);

        Assert.Null(row1);
        Assert.Null(row2);
    }

    [Fact]
    public async Task Stream_ReadingMore1000RowsFromChannel_ReturnChannelExecuteParts()
    {
        const int sizeSeasons = 10_000;
        var tasks = new Task[sizeSeasons];

        for (uint i = 0; i < sizeSeasons; i++)
        {
            tasks[i] = _queryClient.Exec(
                $"INSERT INTO {Tables.Seasons} (series_id, season_id, title) VALUES (3, $season_id, $title)",
                new Dictionary<string, YdbValue>
                {
                    { "$season_id", YdbValue.MakeUint64(i) },
                    { "$title", YdbValue.MakeUtf8("Season " + i) }
                });
        }

        await Task.WhenAll(tasks);

        var currentSeason = 0;

        await _queryClient.Stream(
            $"SELECT title FROM {Tables.Seasons} ORDER BY series_id, season_id LIMIT {sizeSeasons} OFFSET 9",
            async stream =>
            {
                await foreach (var part in stream)
                {
                    foreach (var row in part.ResultSet!.Rows)
                    {
                        Assert.Equal("Season " + currentSeason++, row[0].GetOptionalUtf8());
                    }
                }
            });
    }

    public async Task InitializeAsync()
    {
        await _queryClient.Exec(Tables.CreateTables);
        await _queryClient.Exec(Tables.UpsertData);
    }

    public async Task DisposeAsync()
    {
        await _queryClient.Exec(Tables.DeleteTables);
    }
}
