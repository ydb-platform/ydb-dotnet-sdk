using System.Data;
using AdoNet;
using CommandLine;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;

using var factory = LoggerFactory.Create(builder => builder.AddConsole());

await Parser.Default.ParseArguments<CmdOptions>(args)
    .WithParsedAsync(cmd => new AppContext(factory.CreateLogger<AppContext>(), cmd).Run());

internal class AppContext
{
    private readonly ILogger<AppContext> _logger;
    private readonly CmdOptions _cmdOptions;

    internal AppContext(ILogger<AppContext> logger, CmdOptions cmdOptions)
    {
        _logger = logger;
        _cmdOptions = cmdOptions;
    }

    internal async Task Run()
    {
        _logger.LogInformation("Start app example");

        await InitTables();
        await LoadData();
        await SelectWithParameters();
        await CreatingUser();

        _logger.LogInformation("Clearing all pools...");

        await YdbConnection.ClearAllPools();

        _logger.LogInformation("Cleared all pools");

        await InteractiveTransaction();
        await TlsConnectionExample();
        await ReadResultSetsWithUserPassword();
        await ConnectionWithLoggerFactory();

        _logger.LogInformation("Finish app example");
    }

    private async Task InitTables()
    {
        // YdbConnection from connection string
        await using var connection = new YdbConnection(_cmdOptions.SimpleConnectionString);
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = """
                                 CREATE TABLE series
                                 (
                                     series_id Uint64,
                                     title Text,
                                     series_info Text,
                                     release_date Date,
                                     PRIMARY KEY (series_id)
                                 );

                                 CREATE TABLE seasons
                                 (
                                     series_id Uint64,
                                     season_id Uint64,
                                     title Text,
                                     first_aired Date,
                                     last_aired Date,
                                     PRIMARY KEY (series_id, season_id)
                                 );

                                 CREATE TABLE episodes
                                 (
                                     series_id Uint64,
                                     season_id Uint64,
                                     episode_id Uint64,
                                     title Text,
                                     air_date Date,
                                     PRIMARY KEY (series_id, season_id, episode_id)
                                 );
                                 """;
        _logger.LogInformation("Creating tables for examples, SQL script: {CommandText}", ydbCommand.CommandText);

        await ydbCommand.ExecuteNonQueryAsync();

        _logger.LogInformation("Created tables");
    }

    private async Task LoadData()
    {
        // YdbConnection from YdbConnectionStringBuilder
        await using var connection = new YdbConnection(new YdbConnectionStringBuilder
            { Host = _cmdOptions.Host, Port = _cmdOptions.Port, Database = _cmdOptions.Database });
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = """
                                    UPSERT INTO series (series_id, title, release_date, series_info)
                                    VALUES (1, "IT Crowd", Date("2006-02-03"),
                                    "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
                                    (2, "Silicon Valley", Date("2014-04-06"),
                                    "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.");
                                    UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired)
                                    VALUES (1, 1, "Season 1", Date("2006-02-03"), Date("2006-03-03")),
                                        (1, 2, "Season 2", Date("2007-08-24"), Date("2007-09-28")),
                                        (1, 3, "Season 3", Date("2008-11-21"), Date("2008-12-26")),
                                        (1, 4, "Season 4", Date("2010-06-25"), Date("2010-07-30")),
                                        (2, 1, "Season 1", Date("2014-04-06"), Date("2014-06-01")),
                                        (2, 2, "Season 2", Date("2015-04-12"), Date("2015-06-14")),
                                        (2, 3, "Season 3", Date("2016-04-24"), Date("2016-06-26")),
                                        (2, 4, "Season 4", Date("2017-04-23"), Date("2017-06-25")),
                                        (2, 5, "Season 5", Date("2018-03-25"), Date("2018-05-13"));
                                    UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
                                    VALUES (1, 1, 1, "Yesterday's Jam", Date("2006-02-03")),
                                        (1, 1, 2, "Calamity Jen", Date("2006-02-03")),
                                        (1, 1, 3, "Fifty-Fifty", Date("2006-02-10")),
                                        (1, 1, 4, "The Red Door", Date("2006-02-17")),
                                        (1, 1, 5, "The Haunting of Bill Crouse", Date("2006-02-24")),
                                        (1, 1, 6, "Aunt Irma Visits", Date("2006-03-03")),
                                        (1, 2, 1, "The Work Outing", Date("2006-08-24")),
                                        (1, 2, 2, "Return of the Golden Child", Date("2007-08-31")),
                                        (1, 2, 3, "Moss and the German", Date("2007-09-07")),
                                        (1, 2, 4, "The Dinner Party", Date("2007-09-14")),
                                        (1, 2, 5, "Smoke and Mirrors", Date("2007-09-21")),
                                        (1, 2, 6, "Men Without Women", Date("2007-09-28")),
                                        (1, 3, 1, "From Hell", Date("2008-11-21")),
                                        (1, 3, 2, "Are We Not Men?", Date("2008-11-28")),
                                        (1, 3, 3, "Tramps Like Us", Date("2008-12-05")),
                                        (1, 3, 4, "The Speech", Date("2008-12-12")),
                                        (1, 3, 5, "Friendface", Date("2008-12-19")),
                                        (1, 3, 6, "Calendar Geeks", Date("2008-12-26")),
                                        (1, 4, 1, "Jen The Fredo", Date("2010-06-25")),
                                        (1, 4, 2, "The Final Countdown", Date("2010-07-02")),
                                        (1, 4, 3, "Something Happened", Date("2010-07-09")),
                                        (1, 4, 4, "Italian For Beginners", Date("2010-07-16")),
                                        (1, 4, 5, "Bad Boys", Date("2010-07-23")),
                                        (1, 4, 6, "Reynholm vs Reynholm", Date("2010-07-30")),
                                        (2, 1, 1, "Minimum Viable Product", Date("2014-04-06")),
                                        (2, 1, 2, "The Cap Table", Date("2014-04-13")),
                                        (2, 1, 3, "Articles of Incorporation", Date("2014-04-20")),
                                        (2, 1, 4, "Fiduciary Duties", Date("2014-04-27")),
                                        (2, 1, 5, "Signaling Risk", Date("2014-05-04")),
                                        (2, 1, 6, "Third Party Insourcing", Date("2014-05-11")),
                                        (2, 1, 7, "Proof of Concept", Date("2014-05-18")),
                                        (2, 1, 8, "Optimal Tip-to-Tip Efficiency", Date("2014-06-01")),
                                        (2, 2, 1, "Sand Hill Shuffle", Date("2015-04-12")),
                                        (2, 2, 2, "Runaway Devaluation", Date("2015-04-19")),
                                        (2, 2, 3, "Bad Money", Date("2015-04-26")),
                                        (2, 2, 4, "The Lady", Date("2015-05-03")),
                                        (2, 2, 5, "Server Space", Date("2015-05-10")),
                                        (2, 2, 6, "Homicide", Date("2015-05-17")),
                                        (2, 2, 7, "Adult Content", Date("2015-05-24")),
                                        (2, 2, 8, "White Hat/Black Hat", Date("2015-05-31")),
                                        (2, 2, 9, "Binding Arbitration", Date("2015-06-07")),
                                        (2, 2, 10, "Two Days of the Condor", Date("2015-06-14")),
                                        (2, 3, 1, "Founder Friendly", Date("2016-04-24")),
                                        (2, 3, 2, "Two in the Box", Date("2016-05-01")),
                                        (2, 3, 3, "Meinertzhagen's Haversack", Date("2016-05-08")),
                                        (2, 3, 4, "Maleant Data Systems Solutions", Date("2016-05-15")),
                                        (2, 3, 5, "The DefaultInstance Chair", Date("2016-05-22")),
                                        (2, 3, 6, "Bachmanity Insanity", Date("2016-05-29")),
                                        (2, 3, 7, "To Build a Better Beta", Date("2016-06-05")),
                                        (2, 3, 8, "Bachman's Earnings Over-Ride", Date("2016-06-12")),
                                        (2, 3, 9, "Daily Active Users", Date("2016-06-19")),
                                        (2, 3, 10, "The Uptick", Date("2016-06-26")),
                                        (2, 4, 1, "Success Failure", Date("2017-04-23")),
                                        (2, 4, 2, "Terms of Service", Date("2017-04-30")),
                                        (2, 4, 3, "Intellectual Property", Date("2017-05-07")),
                                        (2, 4, 4, "Teambuilding Exercise", Date("2017-05-14")),
                                        (2, 4, 5, "The Blood Boy", Date("2017-05-21")),
                                        (2, 4, 6, "Customer Service", Date("2017-05-28")),
                                        (2, 4, 7, "The Patent Troll", Date("2017-06-04")),
                                        (2, 4, 8, "The Keenan Vortex", Date("2017-06-11")),
                                        (2, 4, 9, "Hooli-Con", Date("2017-06-18")),
                                        (2, 4, 10, "Server Error", Date("2017-06-25")),
                                        (2, 5, 1, "Grow Fast or Die Slow", Date("2018-03-25")),
                                        (2, 5, 2, "Reorientation", Date("2018-04-01")),
                                        (2, 5, 3, "Chief Operating Officer", Date("2018-04-08")),
                                        (2, 5, 4, "Tech Evangelist", Date("2018-04-15")),
                                        (2, 5, 5, "Facial Recognition", Date("2018-04-22")),
                                        (2, 5, 6, "Artificial Emotional Intelligence", Date("2018-04-29")),
                                        (2, 5, 7, "Initial Coin Offering", Date("2018-05-06")),
                                        (2, 5, 8, "Fifty-One Percent", Date("2018-05-13"));
                                 """;
        _logger.LogInformation("Loading data for examples, SQL script: {CommandText}", ydbCommand.CommandText);

        await ydbCommand.ExecuteNonQueryAsync();

        _logger.LogInformation("Loaded data");
    }

    private async Task SelectWithParameters()
    {
        await using var connection = new YdbConnection(_cmdOptions.SimpleConnectionString);
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = """
                                 SELECT
                                    series_id,
                                    season_id,
                                    episode_id,
                                    air_date, 
                                    title
                                 FROM episodes
                                 WHERE
                                    series_id = $series_id      -- List of conditions to build the result
                                    AND season_id > $season_id  -- Logical AND is used for complex conditions
                                 ORDER BY              -- Sorting the results.
                                    series_id,         -- ORDER BY sorts the values by one or multiple
                                    season_id,         -- columns. Columns are separated by commas.
                                    episode_id
                                 LIMIT $limit_size      -- LIMIT N after ORDER BY means
                                                       -- "get top N" or "get bottom N" results,
                                 ;                     -- depending on sort order.
                                 """;
        ydbCommand.Parameters.Add(new YdbParameter("$series_id", DbType.UInt64, 1));
        ydbCommand.Parameters.Add(new YdbParameter("$season_id", DbType.UInt64, 1));
        ydbCommand.Parameters.Add(new YdbParameter("$limit_size", DbType.UInt64, 3));

        _logger.LogInformation("Selecting data, SQL script: {CommandText}", ydbCommand.CommandText);

        var ydbDataReader = ydbCommand.ExecuteReader();

        _logger.LogInformation("Selected rows:");
        while (await ydbDataReader.ReadAsync())
        {
            _logger.LogInformation(
                "series_id: {series_id}, season_id: {season_id}, episode_id: {episode_id}, air_date: {air_date}, title: {title}",
                ydbDataReader.GetUint64(0), ydbDataReader.GetUint64(1), ydbDataReader.GetUint64(2),
                ydbDataReader.GetDateTime(3), ydbDataReader.GetString(4));
        }
    }

    private async Task CreatingUser()
    {
        await using var ydbConnection = new YdbConnection(_cmdOptions.SimpleConnectionString);
        await ydbConnection.OpenAsync();

        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = "CREATE USER user PASSWORD 'password'";

        _logger.LogInformation("Creating user with password: [user, password]");
        await ydbCommand.ExecuteNonQueryAsync();
        _logger.LogInformation("Created user, next steps will be using the user with a password");
    }

    private async Task InteractiveTransaction()
    {
        await using var ydbConnection = new YdbConnection(_cmdOptions.SimpleConnectionString);
        await ydbConnection.OpenAsync();

        var ydbCommand = ydbConnection.CreateCommand();

        ydbCommand.Transaction = ydbConnection.BeginTransaction();
        ydbCommand.CommandText = """
                                    UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
                                    VALUES (2, 5, 13, "Test Episode", Date("2018-08-27"))
                                 """;
        _logger.LogInformation("Updating data, SQL script: {CommandText}", ydbCommand.CommandText);
        await ydbCommand.ExecuteNonQueryAsync();

        ydbCommand.CommandText = """
                                 INSERT INTO episodes(series_id, season_id, episode_id, title, air_date)
                                 VALUES 
                                     (2, 5, 21, "Test 21", Date("2018-08-27")),
                                     (2, 5, 22, "Test 22", Date("2018-08-27"))
                                 """;
        _logger.LogInformation("Inserting data, SQL script: {CommandText}", ydbCommand.CommandText);
        await ydbCommand.ExecuteNonQueryAsync();
        await ydbCommand.Transaction.CommitAsync();
        _logger.LogInformation("Commit transaction");

        ydbCommand.CommandText = "SELECT title FROM episodes WHERE series_id = 2 AND season_id = 5 AND episode_id = 21";
        _logger.LogInformation("New episode title: {}", ydbCommand.ExecuteScalar());

        ydbCommand.CommandText = "SELECT title FROM episodes WHERE series_id = 2 AND season_id = 5 AND episode_id = 22";
        _logger.LogInformation("New episode title: {}", ydbCommand.ExecuteScalar());

        ydbCommand.CommandText = "SELECT title FROM episodes WHERE series_id = 2 AND season_id = 5 AND episode_id = 21";
        _logger.LogInformation("New episode title: {}", ydbCommand.ExecuteScalar());

        ydbCommand.CommandText = "SELECT title FROM episodes WHERE series_id = 2 AND season_id = 5 AND episode_id = 13";
        _logger.LogInformation("Updated episode title: {}", ydbCommand.ExecuteScalar());
    }

    private async Task TlsConnectionExample()
    {
        if (!_cmdOptions.UseTls)
        {
            _logger.LogInformation("Tls example was ignored");
            return;
        }

        await using var ydbConnection = new YdbConnection(
            $"Host={_cmdOptions.Host};Port={_cmdOptions.TlsPort};RootCertificate=" +
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ca.pem"));

        var command = ydbConnection.CreateCommand();
        command.CommandText = """
                              SELECT
                                  sa.title AS season_title,    -- sa and sr are "join names",
                                  sr.title AS series_title,    -- table aliases declared below using AS.
                                  sr.series_id,                -- They are used to avoid
                                  sa.season_id                 -- ambiguity in the column names used.
                              FROM
                                  seasons AS sa
                              INNER JOIN
                                  series AS sr
                              ON sa.series_id = sr.series_id
                              WHERE sa.series_id = 1
                              ORDER BY                         -- Sorting of the results.
                                  sr.series_id,
                                  sa.season_id                 -- ORDER BY sorts the values by one column
                              ;
                              """;
        var ydbDataReader = command.ExecuteReader();

        while (await ydbDataReader.ReadAsync())
        {
            _logger.LogInformation("season_title: {}, series_title: {}, series_id: {}, season_id: {}",
                ydbDataReader.GetString("season_title"), ydbDataReader.GetString("series_title"),
                ydbDataReader.GetUint64(2), ydbDataReader.GetUint64(3));
        }
    }

    private async Task ReadResultSetsWithUserPassword()
    {
        await using var ydbConnection =
            new YdbConnection($"{_cmdOptions.SimpleConnectionString};User=user;Password=password");
        await ydbConnection.OpenAsync();

        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1; SELECT 2; SELECT 3";

        var ydbDataReader = ydbCommand.ExecuteReader();

        while (await ydbDataReader.NextResultAsync())
        {
            while (await ydbDataReader.ReadAsync())
            {
                _logger.LogInformation("Iterate by resultSets: {}", ydbDataReader.GetValue(0));
            }
        }
    }

    private async Task ConnectionWithLoggerFactory()
    {
        var connectionStringBuilder = new YdbConnectionStringBuilder
        {
            Host = _cmdOptions.Host,
            Port = _cmdOptions.Port,
            LoggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug))
        };
        await using var ydbConnection = new YdbConnection(connectionStringBuilder);
        await ydbConnection.OpenAsync();

        var ydbCommand = ydbConnection.CreateCommand();

        _logger.LogInformation("Dropping tables of examples");
        ydbCommand.CommandText = "DROP TABLE episodes; DROP TABLE seasons; DROP TABLE series; DROP USER user;";
        _logger.LogInformation("Dropped tables of examples");

        await ydbCommand.ExecuteNonQueryAsync();
    }
}