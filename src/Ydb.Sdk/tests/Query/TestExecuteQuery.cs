using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests.Query;

internal record Series(ulong SeriesId, string Title, DateTime ReleaseDate, string Info)
{
    internal static Series FromRow(Sdk.Value.ResultSet.Row row)
    {
        return new Series(
            SeriesId: (ulong)row["series_id"],
            Title: (string)row["title"]!,
            ReleaseDate: (DateTime)row["release_date"].GetOptionalDate()!,
            Info: (string)row["series_info"]!
        );
    }
}

internal record Season(ulong SeriesId, ulong SeasonId, string Title, DateTime FirstAired, DateTime LastAired);

internal record Episode(ulong SeriesId, ulong SeasonId, ulong EpisodeId, string Title, DateTime AirDate);

[Trait("Category", "Integration")]
public class TestExecuteQuery
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly ILoggerFactory _loggerFactory;

    private readonly DriverConfig _driverConfig = new(
        endpoint: "grpc://localhost:2136",
        database: "/local"
    );

    public TestExecuteQuery(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
        _loggerFactory = Utils.GetLoggerFactory() ?? NullLoggerFactory.Instance;
        _loggerFactory.CreateLogger<TestExecuteQuery>();
    }


    [Fact]
    public async Task Query()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        await CreateTestData(driver);

        var response = await client.Query(
            queryString: "SELECT * FROM series WHERE title = $title",
            parameters: new Dictionary<string, YdbValue>
            {
                { "$title", YdbValue.MakeUtf8("IT Crowd") }
            },
            func: ReadSeries
            // txModeSettings: new TxModeOnlineSettings() // default SerializableRW
        );

        response.EnsureSuccess();
        if (response.Result is not null)
        {
            foreach (var series in response.Result)
            {
                _testOutputHelper.WriteLine(series.ToString());
            }
        }
    }

    // not implemented fully yet because server doesnt support transaction methods
    // (BeginTransaction, CommitTransaction, Rollback transaction) in query service
    // currently returns ClientTransportTimeout exception 
    [Fact]
    public async Task Tx()
    {
        await using var driver = await Driver.CreateInitialized(_driverConfig, _loggerFactory);
        using var client = new QueryClient(driver);

        await CreateTestData(driver);

        var response = await client.DoTx(
            func: async tx =>
            {
                // read series
                var seriesResponse = await tx.Query(
                    queryString: "SELECT * FROM series",
                    func: ReadSeries
                );
                seriesResponse.EnsureSuccess();

                // read titles of seasons in "IT Crowd" series
                var series = seriesResponse.Result;
                if (series is null) return new List<string>();
                var itCrowdId = series.First(x => x.Title == "IT Crowd").SeriesId;

                var itCrowdSeasonTitles = await tx.Query(
                    queryString: "SELECT title FROM seasons WHERE series_id = $series_id",
                    parameters: new Dictionary<string, YdbValue>
                    {
                        { "$series_id", (YdbValue)itCrowdId }
                    },
                    func: async stream =>
                    {
                        var titles = new List<string>();
                        await foreach (var part in stream)
                        {
                            var resultSet = part.ResultSet;
                            if (resultSet is not null)
                            {
                                titles.AddRange(resultSet.Rows.Select(row => row["title"].GetUtf8()));
                            }
                        }

                        return titles;
                    }
                );
                itCrowdSeasonTitles.EnsureSuccess();
                var result = itCrowdSeasonTitles.Result;

                return result;
            }
            // txModeSettings: new TxModeOnlineSettings() // default SerializableRW
        );
        response.EnsureSuccess();
        if (response.Result is not null)
        {
            _testOutputHelper.WriteLine(string.Join('\n', response.Result));
        }
    }


    private const string CreateTableQuery = @"
CREATE TABLE series (
    series_id Uint64 NOT NULL,
    title Utf8,
    series_info Utf8,
    release_date Date,
    PRIMARY KEY (series_id)
);

CREATE TABLE seasons (
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Date,
    last_aired Date,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes (
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Date,
    PRIMARY KEY (series_id, season_id, episode_id)
);";

    internal static Dictionary<string, YdbValue> GetDataParams()
    {
        var series = new Series[]
        {
            new(SeriesId: 1, Title: "IT Crowd", ReleaseDate: DateTime.Parse("2006-02-03"),
                Info: "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, " +
                      "produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, " +
                      "and Matt Berry."),
            new(SeriesId: 2, Title: "Silicon Valley", ReleaseDate: DateTime.Parse("2014-04-06"),
                Info: "Silicon Valley is an American comedy television series created by Mike Judge, " +
                      "John Altschuler and Dave Krinsky. The series focuses on five young men who founded " +
                      "a startup company in Silicon Valley.")
        };

        var seasons = new Season[]
        {
            new(1, 1, "Season 1", DateTime.Parse("2006-02-03"), DateTime.Parse("2006-03-03")),
            new(1, 2, "Season 2", DateTime.Parse("2007-08-24"), DateTime.Parse("2007-09-28")),
            new(1, 3, "Season 3", DateTime.Parse("2008-11-21"), DateTime.Parse("2008-12-26")),
            new(1, 4, "Season 4", DateTime.Parse("2010-06-25"), DateTime.Parse("2010-07-30")),
            new(2, 1, "Season 1", DateTime.Parse("2014-04-06"), DateTime.Parse("2014-06-01")),
            new(2, 2, "Season 2", DateTime.Parse("2015-04-12"), DateTime.Parse("2015-06-14")),
            new(2, 3, "Season 3", DateTime.Parse("2016-04-24"), DateTime.Parse("2016-06-26")),
            new(2, 4, "Season 4", DateTime.Parse("2017-04-23"), DateTime.Parse("2017-06-25")),
            new(2, 5, "Season 5", DateTime.Parse("2018-03-25"), DateTime.Parse("2018-05-13")),
        };

        var episodes = new Episode[]
        {
            new(1, 1, 1, "Yesterday's Jam", DateTime.Parse("2006-02-03")),
            new(1, 1, 2, "Calamity Jen", DateTime.Parse("2006-02-03")),
            new(1, 1, 3, "Fifty-Fifty", DateTime.Parse("2006-02-10")),
            new(1, 1, 4, "The Red Door", DateTime.Parse("2006-02-17")),
            new(1, 1, 5, "The Haunting of Bill Crouse", DateTime.Parse("2006-02-24")),
            new(1, 1, 6, "Aunt Irma Visits", DateTime.Parse("2006-03-03")),
            new(1, 2, 1, "The Work Outing", DateTime.Parse("2006-08-24")),
            new(1, 2, 2, "Return of the Golden Child", DateTime.Parse("2007-08-31")),
            new(1, 2, 3, "Moss and the German", DateTime.Parse("2007-09-07")),
            new(1, 2, 4, "The Dinner Party", DateTime.Parse("2007-09-14")),
            new(1, 2, 5, "Smoke and Mirrors", DateTime.Parse("2007-09-21")),
            new(1, 2, 6, "Men Without Women", DateTime.Parse("2007-09-28")),
            new(1, 3, 1, "From Hell", DateTime.Parse("2008-11-21")),
            new(1, 3, 2, "Are We Not Men?", DateTime.Parse("2008-11-28")),
            new(1, 3, 3, "Tramps Like Us", DateTime.Parse("2008-12-05")),
            new(1, 3, 4, "The Speech", DateTime.Parse("2008-12-12")),
            new(1, 3, 5, "Friendface", DateTime.Parse("2008-12-19")),
            new(1, 3, 6, "Calendar Geeks", DateTime.Parse("2008-12-26")),
            new(1, 4, 1, "Jen The Fredo", DateTime.Parse("2010-06-25")),
            new(1, 4, 2, "The Final Countdown", DateTime.Parse("2010-07-02")),
            new(1, 4, 3, "Something Happened", DateTime.Parse("2010-07-09")),
            new(1, 4, 4, "Italian For Beginners", DateTime.Parse("2010-07-16")),
            new(1, 4, 5, "Bad Boys", DateTime.Parse("2010-07-23")),
            new(1, 4, 6, "Reynholm vs Reynholm", DateTime.Parse("2010-07-30")),
            new(2, 1, 1, "Minimum Viable Product", DateTime.Parse("2014-04-06")),
            new(2, 1, 2, "The Cap Table", DateTime.Parse("2014-04-13")),
            new(2, 1, 3, "Articles of Incorporation", DateTime.Parse("2014-04-20")),
            new(2, 1, 4, "Fiduciary Duties", DateTime.Parse("2014-04-27")),
            new(2, 1, 5, "Signaling Risk", DateTime.Parse("2014-05-04")),
            new(2, 1, 6, "Third Party Insourcing", DateTime.Parse("2014-05-11")),
            new(2, 1, 7, "Proof of Concept", DateTime.Parse("2014-05-18")),
            new(2, 1, 8, "Optimal Tip-to-Tip Efficiency", DateTime.Parse("2014-06-01")),
            new(2, 2, 1, "Sand Hill Shuffle", DateTime.Parse("2015-04-12")),
            new(2, 2, 2, "Runaway Devaluation", DateTime.Parse("2015-04-19")),
            new(2, 2, 3, "Bad Money", DateTime.Parse("2015-04-26")),
            new(2, 2, 4, "The Lady", DateTime.Parse("2015-05-03")),
            new(2, 2, 5, "Server Space", DateTime.Parse("2015-05-10")),
            new(2, 2, 6, "Homicide", DateTime.Parse("2015-05-17")),
            new(2, 2, 7, "Adult Content", DateTime.Parse("2015-05-24")),
            new(2, 2, 8, "White Hat/Black Hat", DateTime.Parse("2015-05-31")),
            new(2, 2, 9, "Binding Arbitration", DateTime.Parse("2015-06-07")),
            new(2, 2, 10, "Two Days of the Condor", DateTime.Parse("2015-06-14")),
            new(2, 3, 1, "Founder Friendly", DateTime.Parse("2016-04-24")),
            new(2, 3, 2, "Two in the Box", DateTime.Parse("2016-05-01")),
            new(2, 3, 3, "Meinertzhagen's Haversack", DateTime.Parse("2016-05-08")),
            new(2, 3, 4, "Maleant Data Systems Solutions", DateTime.Parse("2016-05-15")),
            new(2, 3, 5, "The Empty Chair", DateTime.Parse("2016-05-22")),
            new(2, 3, 6, "Bachmanity Insanity", DateTime.Parse("2016-05-29")),
            new(2, 3, 7, "To Build a Better Beta", DateTime.Parse("2016-06-05")),
            new(2, 3, 8, "Bachman's Earnings Over-Ride", DateTime.Parse("2016-06-12")),
            new(2, 3, 9, "Daily Active Users", DateTime.Parse("2016-06-19")),
            new(2, 3, 10, "The Uptick", DateTime.Parse("2016-06-26")),
            new(2, 4, 1, "Success Failure", DateTime.Parse("2017-04-23")),
            new(2, 4, 2, "Terms of Service", DateTime.Parse("2017-04-30")),
            new(2, 4, 3, "Intellectual Property", DateTime.Parse("2017-05-07")),
            new(2, 4, 4, "Teambuilding Exercise", DateTime.Parse("2017-05-14")),
            new(2, 4, 5, "The Blood Boy", DateTime.Parse("2017-05-21")),
            new(2, 4, 6, "Customer Service", DateTime.Parse("2017-05-28")),
            new(2, 4, 7, "The Patent Troll", DateTime.Parse("2017-06-04")),
            new(2, 4, 8, "The Keenan Vortex", DateTime.Parse("2017-06-11")),
            new(2, 4, 9, "Hooli-Con", DateTime.Parse("2017-06-18")),
            new(2, 4, 10, "Server Error", DateTime.Parse("2017-06-25")),
            new(2, 5, 1, "Grow Fast or Die Slow", DateTime.Parse("2018-03-25")),
            new(2, 5, 2, "Reorientation", DateTime.Parse("2018-04-01")),
            new(2, 5, 3, "Chief Operating Officer", DateTime.Parse("2018-04-08")),
            new(2, 5, 4, "Tech Evangelist", DateTime.Parse("2018-04-15")),
            new(2, 5, 5, "Facial Recognition", DateTime.Parse("2018-04-22")),
            new(2, 5, 6, "Artificial Emotional Intelligence", DateTime.Parse("2018-04-29")),
            new(2, 5, 7, "Initial Coin Offering", DateTime.Parse("2018-05-06")),
            new(2, 5, 8, "Fifty-One Percent", DateTime.Parse("2018-05-13")),
        };

        var seriesData = series.Select(s => YdbValue.MakeStruct(new Dictionary<string, YdbValue>
        {
            { "series_id", YdbValue.MakeUint64(s.SeriesId) },
            { "title", YdbValue.MakeUtf8(s.Title) },
            { "series_info", YdbValue.MakeUtf8(s.Info) },
            { "release_date", YdbValue.MakeDate(s.ReleaseDate) }
        })).ToList();

        var seasonsData = seasons.Select(s => YdbValue.MakeStruct(new Dictionary<string, YdbValue>
        {
            { "series_id", YdbValue.MakeUint64(s.SeriesId) },
            { "season_id", YdbValue.MakeUint64(s.SeasonId) },
            { "title", YdbValue.MakeUtf8(s.Title) },
            { "first_aired", YdbValue.MakeDate(s.FirstAired) },
            { "last_aired", YdbValue.MakeDate(s.LastAired) }
        })).ToList();

        var episodesData = episodes.Select(e => YdbValue.MakeStruct(new Dictionary<string, YdbValue>
        {
            { "series_id", YdbValue.MakeUint64(e.SeriesId) },
            { "season_id", YdbValue.MakeUint64(e.SeasonId) },
            { "episode_id", YdbValue.MakeUint64(e.EpisodeId) },
            { "title", YdbValue.MakeUtf8(e.Title) },
            { "air_date", YdbValue.MakeDate(e.AirDate) }
        })).ToList();

        return new Dictionary<string, YdbValue>
        {
            { "$seriesData", YdbValue.MakeList(seriesData) },
            { "$seasonsData", YdbValue.MakeList(seasonsData) },
            { "$episodesData", YdbValue.MakeList(episodesData) }
        };
    }

    private static async Task CreateTestData(Driver driver)
    {
        using var client = new QueryClient(driver);
        using var tableClient = new TableClient(driver);
        // ddl not working yet in query service so temporary using tableService for ddl queries
        await Utils.ExecuteSchemeQuery(tableClient, CreateTableQuery, ensureSuccess: false);
        // will be replaced by following
        // var createResponse = await client.Query( 
        //     CreateString,
        //     async stream =>
        //     {
        //         while (await stream.Next())
        //         {
        //             var part = stream.Response;
        //             part.EnsureSuccess();
        //         }
        //     },
        // );
        // createResponse.EnsureSuccess();

        var fillResponse = await client.Query(@"
REPLACE INTO series
SELECT * FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT * FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT * FROM AS_TABLE($episodesData);",
            parameters: GetDataParams(),
            func: async stream =>
            {
                while (await stream.Next())
                {
                }
            }
        );
        fillResponse.EnsureSuccess();
    }

    private static async Task<List<Series>> ReadSeries(ExecuteQueryStream stream)
    {
        var series = new List<Series>();
        await foreach (var part in stream)
        {
            var resultSet = part.ResultSet;
            if (resultSet is not null)
            {
                series.AddRange(resultSet.Rows.Select(Series.FromRow));
            }
        }

        return series;
    }
}