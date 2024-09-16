using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using Dapper;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Dapper;

public class DapperIntegrationTests
{
    private static readonly TemporaryTables<DapperIntegrationTests> Tables = new();

    [Fact]
    public async Task DapperYqlTutorialTests()
    {
        SqlMapper.SetTypeMap(
            typeof(Episode),
            new CustomPropertyTypeMap(
                typeof(Episode),
                (type, columnName) =>
                    type.GetProperties().FirstOrDefault(prop =>
                        prop.GetCustomAttributes(false)
                            .OfType<ColumnAttribute>()
                            .Any(attr => attr.Name == columnName)) ?? throw new InvalidOperationException()));

        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        await connection.ExecuteAsync(Tables.CreateTables); // create tables
        await connection.ExecuteAsync(Tables.UpsertData); // adding data to table

        var selectedEpisodes = (await connection.QueryAsync<Episode>($@"
SELECT
   series_id,
   season_id,
   episode_id,
   title,
   air_date

FROM {Tables.Episodes}
WHERE
   series_id = @series_id     -- List of conditions to build the result
   AND season_id > @season_id  -- Logical AND is used for complex conditions

ORDER BY              -- Sorting the results.
   series_id,         -- ORDER BY sorts the values by one or multiple
   season_id,         -- columns. Columns are separated by commas.
   episode_id

LIMIT 3               -- LIMIT N after ORDER BY means
                      -- ""get top N"" or ""get bottom N"" results,
;                     -- depending on sort order.
 ", new { series_id = 1, season_id = 1 })).ToArray();

        Assert.Equal(
            new[]
            {
                new Episode
                {
                    SeriesId = 1, SeasonId = 2, EpisodeId = 1, Title = "The Work Outing",
                    AirDate = new DateTime(2006, 8, 24)
                },
                new Episode
                {
                    SeriesId = 1, SeasonId = 2, EpisodeId = 2, Title = "Return of the Golden Child",
                    AirDate = new DateTime(2007, 8, 31)
                },
                new Episode
                {
                    SeriesId = 1, SeasonId = 2, EpisodeId = 3, Title = "Moss and the German",
                    AirDate = new DateTime(2007, 9, 7)
                }
            }, selectedEpisodes);


        var selectedTitlesSeasonAndSeries = (await connection.QueryAsync<dynamic>($@"
SELECT
    sa.title AS season_title,    -- sa and sr are ""join names"",
    sr.title AS series_title,    -- table aliases declared below using AS.
    sr.series_id,                -- They are used to avoid
    sa.season_id                 -- ambiguity in the column names used.

FROM
    {Tables.Seasons} AS sa
INNER JOIN
    {Tables.Series} AS sr
ON sa.series_id = sr.series_id
WHERE sa.series_id = @series_id
ORDER BY                         -- Sorting of the results.
    sr.series_id,
    sa.season_id                 -- ORDER BY sorts the values by one column
;                                -- or multiple columns.
                                 -- Columns are separated by commas.", new { series_id = 1 })).ToArray();

        for (var i = 0; i < selectedTitlesSeasonAndSeries.Length; i++)
        {
            Assert.Equal("IT Crowd", selectedTitlesSeasonAndSeries[i].series_title);
            Assert.Equal("Season " + (i + 1), selectedTitlesSeasonAndSeries[i].season_title);
        }

        var transaction = connection.BeginTransaction();
        var episode1 = new Episode
            { SeriesId = 2, SeasonId = 5, EpisodeId = 13, Title = "Test Episode", AirDate = new DateTime(2018, 8, 27) };
        var episode2 = new Episode
        {
            SeriesId = 2, SeasonId = 5, EpisodeId = 12, Title = "Test Episode !!!", AirDate = new DateTime(2018, 8, 27)
        };

        var parameters1 = new DynamicParameters();
        parameters1.Add("series_id", episode1.SeriesId, DbType.UInt64);
        parameters1.Add("season_id", episode1.SeasonId, DbType.UInt64);
        parameters1.Add("episode_id", episode1.EpisodeId, DbType.UInt64);
        parameters1.Add("title", episode1.Title, DbType.String);
        parameters1.Add("air_date", episode1.AirDate, DbType.Date);

        await connection.ExecuteAsync($@"
UPSERT INTO {Tables.Episodes}
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    @series_id,
    @season_id,
    @episode_id,
    @title,
    @air_date
);
;", parameters1, transaction);
        await using (var otherConn = new YdbConnection())
        {
            await otherConn.OpenAsync();

            Assert.Null(await otherConn.QuerySingleOrDefaultAsync(
                $"SELECT * FROM {Tables.Episodes} WHERE series_id = @p1 AND season_id = @p2 AND episode_id = @p3",
                new { p1 = episode1.SeriesId, p2 = episode1.SeasonId, p3 = episode1.EpisodeId }));
        }

        var parameters2 = new DynamicParameters();
        parameters2.Add("series_id", episode2.SeriesId, DbType.UInt64);
        parameters2.Add("season_id", episode2.SeasonId, DbType.UInt64);
        parameters2.Add("episode_id", episode2.EpisodeId, DbType.UInt64);
        parameters2.Add("title", episode2.Title, DbType.String);
        parameters2.Add("air_date", episode2.AirDate, DbType.Date);

        await connection.ExecuteAsync($@"
UPSERT INTO {Tables.Episodes}
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    @series_id,
    @season_id,
    @episode_id,
    @title,
    @air_date
);
;", parameters2, transaction);
        await transaction.CommitAsync();

        var rollbackTransaction = connection.BeginTransaction();
        await connection.ExecuteAsync($@"
INSERT INTO {Tables.Episodes}
(
    series_id,
    season_id,
    episode_id,
    title,
    air_date
)
VALUES
(
    2,
    5,
    21,
    ""Test 21"",
    Date(""2018-08-27"")
),                                        -- Rows are separated by commas.
(
    2,
    5,
    22,
    ""Test 22"",
    Date(""2018-08-27"")
)
;
;", transaction: rollbackTransaction);
        await rollbackTransaction.RollbackAsync();

        Assert.Equal((ulong)72, await connection.ExecuteScalarAsync<ulong>($"SELECT COUNT(*) FROM {Tables.Episodes}"));

        await connection.ExecuteAsync(Tables.DeleteTables);
    }

    private record Episode
    {
        [Column("series_id")] public uint SeriesId { get; set; }
        [Column("season_id")] public uint SeasonId { get; set; }
        [Column("episode_id")] public uint EpisodeId { get; set; }
        [Column("title")] public string Title { get; set; } = null!;
        [Column("air_date")] public DateTime AirDate { get; set; }
    }
}