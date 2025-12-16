using LinqToDB;
using LinqToDB.Async;
using LinqToDB.Data;
using LinqToDB.Mapping;
using Microsoft.Extensions.Logging;

using var factory = LoggerFactory.Create(b => b.AddConsole());
var app = new AppContext(factory.CreateLogger<AppContext>());
await app.Run();

#region LINQ2DB MODELS

[Table("series")]
public sealed class Series
{
    [PrimaryKey] [Column("series_id")] public ulong SeriesId { get; init; }

    [Column("title")] [NotNull] public string Title { get; init; } = null!;

    [Column("series_info")] public string? SeriesInfo { get; init; }

    [Column("release_date")]
    [DataType(DataType.Date)]
    public DateTime ReleaseDate { get; init; }
}

[Table("seasons")]
public sealed class Season
{
    [PrimaryKey] [Column("series_id")] public ulong SeriesId { get; init; }

    [PrimaryKey] [Column("season_id")] public ulong SeasonId { get; init; }

    [Column("title")] [NotNull] public string Title { get; init; } = null!;

    [Column("first_aired")]
    [DataType(DataType.Date)]
    public DateTime FirstAired { get; init; }

    [Column("last_aired")]
    [DataType(DataType.Date)]
    public DateTime LastAired { get; init; }
}

[Table("episodes")]
public sealed class Episode
{
    [PrimaryKey] [Column("series_id")] public ulong SeriesId { get; init; }

    [PrimaryKey] [Column("season_id")] public ulong SeasonId { get; init; }

    [PrimaryKey] [Column("episode_id")] public ulong EpisodeId { get; init; }

    [Column("title")] [NotNull] public string Title { get; init; } = null!;

    [Column("air_date")]
    [DataType(DataType.Date)]
    public DateTime AirDate { get; init; }
}

#endregion

#region LINQ2DB DATACONTEXT

internal sealed class MyYdb(DataOptions options) : DataConnection(options)
{
    public ITable<Series> Series => this.GetTable<Series>();
    public ITable<Season> Seasons => this.GetTable<Season>();
    public ITable<Episode> Episodes => this.GetTable<Episode>();
}

#endregion

#region SETTINGS

internal sealed record Settings(
    string Host,
    int Port,
    string Database,
    bool UseTls,
    int TlsPort)
{
    public string SimpleConnectionString =>
        $"Host={Host};Port={(UseTls ? TlsPort : Port)};Database={Database};UseTls={(UseTls ? "true" : "false")}";
}

internal static class SettingsLoader
{
    public static Settings Load()
    {
        var host = Environment.GetEnvironmentVariable("YDB_HOST") ?? "localhost";
        var port = TryInt(Environment.GetEnvironmentVariable("YDB_PORT"), 2136);
        var db = Environment.GetEnvironmentVariable("YDB_DB") ?? "/local";
        var useTls = TryBool(Environment.GetEnvironmentVariable("YDB_USE_TLS"), false);
        var tls = TryInt(Environment.GetEnvironmentVariable("YDB_TLS_PORT"), 2135);

        return new Settings(host, port, db, useTls, tls);

        static int TryInt(string? s, int d)
        {
            return int.TryParse(s, out var v) ? v : d;
        }

        static bool TryBool(string? s, bool d)
        {
            return bool.TryParse(s, out var v) ? v : d;
        }
    }
}

#endregion

internal class AppContext(ILogger<AppContext> logger)
{
    private readonly Settings _settings = SettingsLoader.Load();

    private DataOptions BuildOptions(string? overrideConnectionString = null)
    {
        var cs = overrideConnectionString ?? _settings.SimpleConnectionString;
        return new DataOptions().UseConnectionString("YDB", cs);
    }

    public async Task Run()
    {
        logger.LogInformation("Start app example");

        await InitTables();
        await LoadData();
        await SelectWithParameters();
        await Select();

        await InteractiveTransaction();
        await TlsConnectionExample();
        await ConnectionWithLoggerFactory();

        logger.LogInformation("Finish app example");
    }

    private async Task InitTables()
    {
        await using var db = new MyYdb(BuildOptions());
        try
        {
            await db.CreateTableAsync<Series>();
        }
        catch
        {
            logger.LogDebug("series exists");
        }

        try
        {
            await db.CreateTableAsync<Season>();
        }
        catch
        {
            logger.LogDebug("seasons exists");
        }

        try
        {
            await db.CreateTableAsync<Episode>();
        }
        catch
        {
            logger.LogDebug("episodes exists");
        }

        logger.LogInformation("Created tables");
    }

    private async Task LoadData()
    {
        await using var db = new MyYdb(BuildOptions());

        var series = new[]
        {
            new Series
            {
                SeriesId = 1, Title = "IT Crowd", ReleaseDate = new DateTime(2006, 02, 03),
                SeriesInfo = "British sitcom..."
            },
            new Series
            {
                SeriesId = 2, Title = "Silicon Valley", ReleaseDate = new DateTime(2014, 04, 06),
                SeriesInfo = "American comedy..."
            }
        };
        foreach (var s in series) await db.InsertAsync(s);

        var seasons = new List<Season>
        {
            new()
            {
                SeriesId = 1, SeasonId = 1, Title = "Season 1", FirstAired = new DateTime(2006, 02, 03),
                LastAired = new DateTime(2006, 03, 03)
            },
            new()
            {
                SeriesId = 1, SeasonId = 2, Title = "Season 2", FirstAired = new DateTime(2007, 08, 24),
                LastAired = new DateTime(2007, 09, 28)
            },
            new()
            {
                SeriesId = 1, SeasonId = 3, Title = "Season 3", FirstAired = new DateTime(2008, 11, 21),
                LastAired = new DateTime(2008, 12, 26)
            },
            new()
            {
                SeriesId = 1, SeasonId = 4, Title = "Season 4", FirstAired = new DateTime(2010, 06, 25),
                LastAired = new DateTime(2010, 07, 30)
            },
            new()
            {
                SeriesId = 2, SeasonId = 1, Title = "Season 1", FirstAired = new DateTime(2014, 04, 06),
                LastAired = new DateTime(2014, 06, 01)
            },
            new()
            {
                SeriesId = 2, SeasonId = 2, Title = "Season 2", FirstAired = new DateTime(2015, 04, 12),
                LastAired = new DateTime(2015, 06, 14)
            },
            new()
            {
                SeriesId = 2, SeasonId = 3, Title = "Season 3", FirstAired = new DateTime(2016, 04, 24),
                LastAired = new DateTime(2016, 06, 26)
            },
            new()
            {
                SeriesId = 2, SeasonId = 4, Title = "Season 4", FirstAired = new DateTime(2017, 04, 23),
                LastAired = new DateTime(2017, 06, 25)
            },
            new()
            {
                SeriesId = 2, SeasonId = 5, Title = "Season 5", FirstAired = new DateTime(2018, 03, 25),
                LastAired = new DateTime(2018, 05, 13)
            }
        };
        await db.BulkCopyAsync(seasons);

        var eps = new List<Episode>
        {
            new()
            {
                SeriesId = 1, SeasonId = 1, EpisodeId = 1, Title = "Yesterday's Jam",
                AirDate = new DateTime(2006, 02, 03)
            },
            new()
            {
                SeriesId = 1, SeasonId = 1, EpisodeId = 2, Title = "Calamity Jen", AirDate = new DateTime(2006, 02, 03)
            },
            new()
            {
                SeriesId = 1, SeasonId = 1, EpisodeId = 3, Title = "Fifty-Fifty", AirDate = new DateTime(2006, 02, 10)
            },
            new()
            {
                SeriesId = 1, SeasonId = 1, EpisodeId = 4, Title = "The Red Door", AirDate = new DateTime(2006, 02, 17)
            },
            new()
            {
                SeriesId = 1, SeasonId = 2, EpisodeId = 1, Title = "The Work Outing",
                AirDate = new DateTime(2007, 08, 24)
            },
            new()
            {
                SeriesId = 1, SeasonId = 2, EpisodeId = 2, Title = "Return of the Golden Child",
                AirDate = new DateTime(2007, 08, 31)
            },
            new()
            {
                SeriesId = 1, SeasonId = 3, EpisodeId = 1, Title = "From Hell", AirDate = new DateTime(2008, 11, 21)
            },
            new()
            {
                SeriesId = 1, SeasonId = 3, EpisodeId = 2, Title = "Are We Not Men?",
                AirDate = new DateTime(2008, 11, 28)
            },
            new()
            {
                SeriesId = 1, SeasonId = 4, EpisodeId = 1, Title = "Jen The Fredo", AirDate = new DateTime(2010, 06, 25)
            },
            new()
            {
                SeriesId = 1, SeasonId = 4, EpisodeId = 2, Title = "The Final Countdown",
                AirDate = new DateTime(2010, 07, 02)
            },
            new()
            {
                SeriesId = 2, SeasonId = 2, EpisodeId = 1, Title = "Minimum Viable Product",
                AirDate = new DateTime(2014, 04, 06)
            },
            new()
            {
                SeriesId = 2, SeasonId = 2, EpisodeId = 2, Title = "The Cap Table", AirDate = new DateTime(2014, 04, 13)
            },
            new()
            {
                SeriesId = 2, SeasonId = 1, EpisodeId = 3, Title = "Articles of Incorporation",
                AirDate = new DateTime(2014, 04, 20)
            },
            new()
            {
                SeriesId = 2, SeasonId = 1, EpisodeId = 4, Title = "Fiduciary Duties",
                AirDate = new DateTime(2014, 04, 27)
            }
        };
        await db.BulkCopyAsync(eps);

        _ = series[0].ReleaseDate.Ticks + (series[0].SeriesInfo?.Length ?? 0);
        _ = seasons[0].FirstAired.Ticks + seasons[0].LastAired.Ticks;

        logger.LogInformation("Loaded data");
    }

    private async Task SelectWithParameters()
    {
        await using var db = new MyYdb(BuildOptions());
        ulong seriesId = 1;
        ulong seasonId = 1;
        ulong limit = 3;

        var rows = await db.Episodes
            .Where(e => e.SeriesId == seriesId && e.SeasonId > seasonId)
            .OrderBy(e => e.SeriesId)
            .ThenBy(e => e.SeasonId)
            .ThenBy(e => e.EpisodeId)
            .Take((int)limit)
            .Select(e => new { e.SeriesId, e.SeasonId, e.EpisodeId, e.AirDate, e.Title })
            .ToListAsync();

        logger.LogInformation("Selected rows:");
        foreach (var r in rows)
            logger.LogInformation(
                "series_id: {series_id}, season_id: {season_id}, episode_id: {episode_id}, air_date: {air_date}, title: {title}",
                r.SeriesId, r.SeasonId, r.EpisodeId, r.AirDate, r.Title);
    }

    private async Task Select()
    {
        await using var db = new MyYdb(BuildOptions());

        var statsRaw = await db.Episodes
            .GroupBy(e => new { e.SeriesId, e.SeasonId })
            .Select(g => new { g.Key.SeriesId, g.Key.SeasonId, Cnt = g.Count() })
            .ToListAsync();

        var stats = statsRaw
            .OrderBy(x => x.SeriesId)
            .ThenBy(x => x.SeasonId);

        foreach (var x in stats)
            logger.LogInformation("series_id: {series_id}, season_id: {season_id}, cnt: {cnt}",
                x.SeriesId, x.SeasonId, x.Cnt);
    }

    private async Task InteractiveTransaction()
    {
        await using var db = new MyYdb(BuildOptions());
        await using var tr = await db.BeginTransactionAsync();

        await db.InsertAsync(new Episode
        {
            SeriesId = 2, SeasonId = 5, EpisodeId = 13, Title = "Test Episode", AirDate = new DateTime(2018, 08, 27)
        });
        await db.InsertAsync(new Episode
            { SeriesId = 2, SeasonId = 5, EpisodeId = 21, Title = "Test 21", AirDate = new DateTime(2018, 08, 27) });
        await db.InsertAsync(new Episode
            { SeriesId = 2, SeasonId = 5, EpisodeId = 22, Title = "Test 22", AirDate = new DateTime(2018, 08, 27) });

        await tr.CommitAsync();
        logger.LogInformation("Commit transaction");

        var title21 = await db.Episodes.Where(e => e.SeriesId == 2 && e.SeasonId == 5 && e.EpisodeId == 21)
            .Select(e => e.Title).SingleAsync();
        logger.LogInformation("New episode title: {title}", title21);

        var title22 = await db.Episodes.Where(e => e.SeriesId == 2 && e.SeasonId == 5 && e.EpisodeId == 22)
            .Select(e => e.Title).SingleAsync();
        logger.LogInformation("New episode title: {title}", title22);

        var title13 = await db.Episodes.Where(e => e.SeriesId == 2 && e.SeasonId == 5 && e.EpisodeId == 13)
            .Select(e => e.Title).SingleAsync();
        logger.LogInformation("Updated episode title: {title}", title13);
    }

    private async Task TlsConnectionExample()
    {
        if (!_settings.UseTls)
        {
            logger.LogInformation("Tls example was ignored");
            return;
        }

        var caPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "ca.pem");
        var tlsCs = $"Host={_settings.Host};Port={_settings.TlsPort};RootCertificate={caPath}";
        await using var db = new MyYdb(BuildOptions(tlsCs));

        var rows = await db.Seasons
            .Where(sa => sa.SeriesId == 1)
            .Join(db.Series, sa => sa.SeriesId, sr => sr.SeriesId,
                (sa, sr) => new { SeasonTitle = sa.Title, SeriesTitle = sr.Title, sr.SeriesId, sa.SeasonId })
            .OrderBy(x => x.SeriesId)
            .ThenBy(x => x.SeasonId)
            .ToListAsync();

        foreach (var r in rows)
            logger.LogInformation(
                "season_title: {SeasonTitle}, series_title: {SeriesTitle}, series_id: {SeriesId}, season_id: {SeasonId}",
                r.SeasonTitle, r.SeriesTitle, r.SeriesId, r.SeasonId);
    }

    private async Task ConnectionWithLoggerFactory()
    {
        var options = BuildOptions($"Host={_settings.Host};Port={_settings.Port}")
            .UseTracing(ti =>
            {
                switch (ti.TraceInfoStep)
                {
                    case TraceInfoStep.BeforeExecute:
                        logger.LogInformation("BeforeExecute: {sql}", ti.SqlText);
                        break;
                    case TraceInfoStep.AfterExecute:
                        logger.LogInformation("AfterExecute: {time} {records} recs", ti.ExecutionTime,
                            ti.RecordsAffected);
                        break;
                    case TraceInfoStep.Error:
                        logger.LogError(ti.Exception, "SQL error");
                        break;
                }
            });

        await using var db = new MyYdb(options);

        logger.LogInformation("Dropping tables of examples");
        try
        {
            await db.DropTableAsync<Episode>();
        }
        catch
        {
            /* ignored */
        }

        try
        {
            await db.DropTableAsync<Season>();
        }
        catch
        {
            /* ignored */
        }

        try
        {
            await db.DropTableAsync<Series>();
        }
        catch
        {
            /* ignored */
        }

        logger.LogInformation("Dropped tables of examples");
    }
}