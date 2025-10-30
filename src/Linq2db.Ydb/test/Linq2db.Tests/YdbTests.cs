#nullable enable

using System.Diagnostics;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Internal.DataProvider.Ydb;
using LinqToDB.Mapping;
using NUnit.Framework;

namespace Linq2db.Tests
{
    [TestFixture]
    public class YdbTests
    
    {
        // --------- Маппинг под твою схему ---------
        [Table("series")]
        sealed class Series
        {
            [PrimaryKey, Column("series_id")]
            public ulong SeriesId { get; set; }

            [Column("title"), NotNull]
            public string Title { get; set; } = null!;

            [Column("series_info")]
            public string? SeriesInfo { get; set; }

            [Column("release_date"), DataType(DataType.Date)]
            public DateTime ReleaseDate { get; set; }
        }

        [Table("seasons")]
        sealed class Season
        {
            [PrimaryKey, Column("series_id")]
            public ulong SeriesId { get; set; }

            [PrimaryKey, Column("season_id")]
            public ulong SeasonId { get; set; }

            [Column("title"), NotNull]
            public string Title { get; set; } = null!;

            [Column("first_aired"), DataType(DataType.Date)]
            public DateTime FirstAired { get; set; }

            [Column("last_aired"), DataType(DataType.Date)]
            public DateTime LastAired { get; set; }
        }

        [Table("episodes")]
        sealed class Episode
        {
            [PrimaryKey, Column("series_id")]
            public ulong SeriesId { get; set; }

            [PrimaryKey, Column("season_id")]
            public ulong SeasonId { get; set; }

            [PrimaryKey, Column("episode_id")]
            public ulong EpisodeId { get; set; }

            [Column("title"), NotNull]
            public string Title { get; set; } = null!;

            [Column("air_date"), DataType(DataType.Date)]
            public DateTime AirDate { get; set; }
        }

        // --------- константы / окружение ---------
        const ulong SeriesIdFixed = 1;
        const ulong SeasonIdFixed = 1;

        DataConnection _db = null!;

        static string BuildConnectionString() => "Host=localhost;Port=2136;Database=/local;UseTls=false;DisableDiscovery=true";

        static int  TryInt (string? s, int d)  => int.TryParse(s, out var v) ? v : d;
        static bool TryBool(string? s, bool d) => bool.TryParse(s, out var v) ? v : d;

        // --------- жизненный цикл тестов ---------
        [SetUp]
        public void SetUp()
        {

            DataConnection.AddProviderDetector(YdbTools.ProviderDetector);
            _db = new DataConnection("YDB", BuildConnectionString());
            DropSchema(_db);
            CreateSchema(_db);
        }


        [TearDown]
        public void TearDown()
        {
            try { DropSchema(_db); } catch { /* ignore */ }
            _db.Dispose();
        }

        static void DropSchema(DataConnection db)
        {
            try { db.DropTable<Episode>(); } catch { }
            try { db.DropTable<Season>();  } catch { }
            try { db.DropTable<Series>();  } catch { }
        }

        static void CreateSchema(DataConnection db)
        {
            db.CreateTable<Series>();
            db.CreateTable<Season>();
            db.CreateTable<Episode>();

            db.Insert(new Series
            {
                SeriesId    = SeriesIdFixed,
                Title       = "Demo Series",
                SeriesInfo  = "Synthetic dataset",
                ReleaseDate = new DateTime(2010, 1, 1)
            });

            db.Insert(new Season
            {
                SeriesId   = SeriesIdFixed,
                SeasonId   = SeasonIdFixed,
                Title      = "Season 1",
                FirstAired = new DateTime(2010, 1, 1),
                LastAired  = new DateTime(2010, 12, 31)
            });
        }

        // ===================== CRUD =====================

        [Test]
        public void CreateSchema_CreatesAllThreeTables()
        {
            // Просто проверим, что можно сделать COUNT по каждой таблице
            Assert.DoesNotThrow(() => _db.GetTable<Series>().Count());
            Assert.DoesNotThrow(() => _db.GetTable<Season>().Count());
            Assert.DoesNotThrow(() => _db.GetTable<Episode>().Count());
        }

        [Test]
        public void Insert_Read_Update_Delete_SingleEpisode()
        {
            var episodes = _db.GetTable<Episode>();

            // CREATE + READ
            var e1 = new Episode
            {
                SeriesId  = SeriesIdFixed,
                SeasonId  = SeasonIdFixed,
                EpisodeId = 1,
                Title     = "Pilot",
                AirDate   = new DateTime(2010, 1, 2)
            };

            Assert.DoesNotThrow(() => _db.Insert(e1));

            var got = episodes.SingleOrDefault(e =>
                e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed && e.EpisodeId == 1);

            Assert.That(got, Is.Not.Null);
            Assert.That(got!.Title, Is.EqualTo("Pilot"));
            Assert.That(got.AirDate, Is.EqualTo(new DateTime(2010, 1, 2)));

            // UPDATE
            var affected = episodes
                .Where(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed && e.EpisodeId == 1)
                .Set(e => e.Title,   _ => "Updated")
                .Set(e => e.AirDate, _ => new DateTime(2010, 1, 3))
                .Update();


            var after = episodes.Single(e =>
                e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed && e.EpisodeId == 1);

            Assert.That(after.Title, Is.EqualTo("Updated"));
            Assert.That(after.AirDate, Is.EqualTo(new DateTime(2010, 1, 3)));

            // DELETE
            episodes.Delete(e =>
                e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed && e.EpisodeId == 1);

            Assert.That(episodes.Any(e =>
                e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed && e.EpisodeId == 1), Is.False);
        }

        [Test]
        public void InsertMany_UpdateAll_DeleteAll_SmallBatch()
        {
            // Чтобы быстро бегало в CI: маленькая партия
            const int batch = 2000;

            var startDate = new DateTime(2010, 1, 1);
            var data = Enumerable.Range(1, batch).Select(i => new Episode
            {
                SeriesId  = SeriesIdFixed,
                SeasonId  = SeasonIdFixed,
                EpisodeId = (ulong)i,
                Title     = $"Episode {i}",
                AirDate   = startDate.AddDays(i % 365),
            });

            var copied = _db.BulkCopy(data);
            Assert.That(copied.RowsCopied, Is.EqualTo(batch));

            var table = _db.GetTable<Episode>();
            Assert.That(table.Count(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed), Is.EqualTo(batch));

            var newTitle = "updated";
            var newDate  = DateTime.UtcNow.Date;

            table
                .Where(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed)
                .Set(e => e.Title,   _ => newTitle)
                .Set(e => e.AirDate, _ => newDate)
                .Update();
            
            var mismatches = table.Count(e =>
                e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed &&
                (e.Title != newTitle || e.AirDate != newDate));

            Assert.That(mismatches, Is.EqualTo(0));

            table.Delete(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed);
            Assert.That(table.Count(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed), Is.EqualTo(0));
        }

        // Тяжёлый e2e-тест на 15к — запускается только вручную
        [Test]
        public void Insert_Update_Delete_15000()
        {
            const int BatchSize = 15_000;
            var episodes = new List<Episode>(BatchSize);
            var startDate = new DateTime(2010, 1, 1);

            for (int i = 1; i <= BatchSize; i++)
            {
                episodes.Add(new Episode
                {
                    SeriesId  = SeriesIdFixed,
                    SeasonId  = SeasonIdFixed,
                    EpisodeId = (ulong)i,
                    Title     = $"Episode {i}",
                    AirDate   = startDate.AddDays(i % 365)
                });
            }

            static T LogTime<T>(string op, Func<T> action)
            {
                var sw = Stopwatch.StartNew();
                try { return action(); }
                finally { sw.Stop(); TestContext.Progress.WriteLine($"{op} | {sw.Elapsed}"); }
            }

            _db.BulkCopy(episodes);

            var table = _db.GetTable<Episode>();
            Assert.That(
                LogTime("COUNT after insert", () => table.Count(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed)),
                Is.EqualTo(BatchSize));

            var newTitle = "updated";
            var newDate  = DateTime.UtcNow.Date;

            LogTime("UPDATE 15k", () =>
                table.Where(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed)
                     .Set(e => e.Title,   _ => newTitle)
                     .Set(e => e.AirDate, _ => newDate)
                     .Update());


            var mismatches = LogTime("Validate updates", () =>
                table.Count(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed &&
                                 (e.Title != newTitle || e.AirDate != newDate)));
            Assert.That(mismatches, Is.EqualTo(0));

            var deleted = LogTime("DELETE 15k", () =>
                table.Delete(e => e.SeriesId == SeriesIdFixed && e.SeasonId == SeasonIdFixed));

            TestContext.Progress.WriteLine($"Deleted reported: {deleted}");
            Assert.That(LogTime("Final COUNT(*)", () => table.Count()), Is.EqualTo(0));
        }
    }
}
