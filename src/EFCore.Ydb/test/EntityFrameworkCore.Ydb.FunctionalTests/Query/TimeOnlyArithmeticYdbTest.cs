using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class TimeOnlyArithmeticYdbTest
{
    [Fact]
    public async Task AddHours_preserves_ticks_and_wraps_midnight()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(TimeOnlyArithmeticYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        TimeOnly[] times = [new(23, 30), TimeOnly.MaxValue, TimeOnly.MinValue];
        context.Entities.AddRange(times.Select((time, i) => new TestEntity
        {
            Id = i + 1, Time = time, TextTime = time
        }));
        await context.SaveChangesAsync();

        Assert.Equal(times, await context.Entities.OrderBy(e => e.Id).Select(e => e.Time).ToArrayAsync());
        double[] offsets =
        [
            1, -1, 0.5, -0.5, 49.5, -49.5, 1e-11, -1e-11, 1e9, -1e9,
            double.NaN, double.PositiveInfinity, double.NegativeInfinity
        ];
        foreach (var hours in offsets)
            await AssertHours(context, times, hours);

        static async Task AssertHours(TestContext db, TimeOnly[] values, double offset)
        {
            var expected = values.Select(time => time.AddHours(offset)).ToArray();
            Assert.Equal(expected, await db.Entities.OrderBy(e => e.Id)
                .Select(e => e.Time.AddHours(offset)).ToArrayAsync());

            var target = expected[0];
            Assert.Equal(values.Select((time, i) => new { Time = time, Id = i + 1 })
                    .Where(e => e.Time.AddHours(offset) == target).Select(e => e.Id),
                await db.Entities.Where(e => e.Time.AddHours(offset) == target)
                    .OrderBy(e => e.Id).Select(e => e.Id).ToArrayAsync());
        }
    }

    [Fact]
    public async Task AddMinutes_preserves_ticks_and_wraps_midnight()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(TimeOnlyArithmeticYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        var time = TimeOnly.MaxValue;
        context.Entities.Add(new TestEntity { Id = 1, Time = time, TextTime = time });
        await context.SaveChangesAsync();

        Assert.Equal(time, await context.Entities.Select(e => e.Time).SingleAsync());

        var minutes = 0.5;
        var expected = time.AddMinutes(minutes);
        Assert.Equal(expected, await context.Entities
            .Where(e => e.Time.AddMinutes(minutes) == expected)
            .Select(e => e.Time.AddMinutes(minutes)).SingleAsync());
    }

    [Fact]
    public void AddHours_with_string_converted_TimeOnly_is_not_translated()
    {
        using var context = new TestContext();
        Assert.Throws<InvalidOperationException>(() => context.Entities
            .Where(e => e.TextTime.AddHours(1) == new TimeOnly(0, 30)).ToQueryString());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public TimeOnly Time { get; set; }
        public TimeOnly TextTime { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) => model.Entity<TestEntity>(entity =>
        {
            entity.ToTable("TimeOnlyArithmeticEntities");
            entity.Property(e => e.Id).ValueGeneratedNever();
            entity.Property(e => e.Time).HasConversion<long>();
            entity.Property(e => e.TextTime).HasConversion<string>();
        });
    }
}
