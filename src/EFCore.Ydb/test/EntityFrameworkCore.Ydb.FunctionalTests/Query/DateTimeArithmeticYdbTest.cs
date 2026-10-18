using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateTimeArithmeticYdbTest
{
    [Fact]
    public async Task DateTime_arithmetic_preserves_timestamp_type_and_time()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateTimeArithmeticYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.Add(new TestEntity
        {
            Id = 1,
            Timestamp = new DateTime(2000, 2, 29, 1, 2, 3).AddTicks(4567890),
            Timestamp64 = new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(4567890)
        });
        await context.SaveChangesAsync();

        var years = 1;
        var expectedYear = new DateTime(2001, 2, 28, 1, 2, 3).AddTicks(4567890);
        var expectedMonth = new DateTime(1968, 1, 29, 1, 2, 3).AddTicks(4567890);

        Assert.Equal([1], await context.Entities
            .Where(e => e.Timestamp.AddYears(years) == expectedYear)
            .Select(e => e.Id)
            .ToListAsync());
        Assert.Equal(new DateTime(1969, 2, 28, 1, 2, 3).AddTicks(4567890), await context.Entities
            .Select(e => e.Timestamp64.AddYears(1))
            .SingleAsync());
        Assert.Equal([1], await context.Entities
            .Where(e => e.Timestamp64.AddMonths(-1) == expectedMonth)
            .Select(e => e.Id)
            .ToListAsync());
        Assert.Equal(new DateTime(2000, 1, 29, 1, 2, 3).AddTicks(4567890), await context.Entities
            .Select(e => e.Timestamp.AddMonths(-1))
            .SingleAsync());
    }

    [Fact]
    public async Task DateTime_elapsed_arithmetic_preserves_fractional_units()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateTimeArithmeticYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.Add(new TestEntity
        {
            Id = 1,
            Timestamp = new DateTime(2000, 2, 29, 1, 2, 3).AddTicks(4567890),
            Timestamp64 = new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(4567890)
        });
        await context.SaveChangesAsync();

        var hours = 1.5;
        var days = -1.25;
        var expectedHours = new DateTime(2000, 2, 29, 2, 32, 3).AddTicks(4567890);
        var expectedDays = new DateTime(1968, 2, 27, 19, 2, 3).AddTicks(4567890);
        var expectedMinutes = new DateTime(2000, 2, 29, 1, 1, 33).AddTicks(4567890);
        var expectedSeconds = new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(7067890);

        Assert.Equal([1], await context.Entities
            .Where(e => e.Timestamp.AddHours(hours) == expectedHours
                        && e.Timestamp64.AddDays(days) == expectedDays
                        && e.Timestamp.AddMinutes(-0.5) == expectedMinutes
                        && e.Timestamp64.AddSeconds(0.25) == expectedSeconds)
            .Select(e => e.Id)
            .ToListAsync());

        var result = await context.Entities.Select(e => new
        {
            Days = e.Timestamp.AddDays(-1.25),
            Hours = e.Timestamp64.AddHours(1.5),
            Minutes = e.Timestamp64.AddMinutes(-0.5),
            Seconds = e.Timestamp.AddSeconds(0.25)
        }).SingleAsync();

        Assert.Equal(new DateTime(2000, 2, 27, 19, 2, 3).AddTicks(4567890), result.Days);
        Assert.Equal(new DateTime(1968, 2, 29, 2, 32, 3).AddTicks(4567890), result.Hours);
        Assert.Equal(new DateTime(1968, 2, 29, 1, 1, 33).AddTicks(4567890), result.Minutes);
        Assert.Equal(new DateTime(2000, 2, 29, 1, 2, 3).AddTicks(7067890), result.Seconds);

        var fineSeconds = 0.000249;
        Assert.Equal(new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(4570380), await context.Entities
            .Select(e => e.Timestamp64.AddSeconds(fineSeconds))
            .SingleAsync());

        var largeDays = 1000000.0000016;
        Assert.Equal(new DateTime(4706, 1, 26, 1, 2, 3).AddTicks(5950300), await context.Entities
            .Select(e => e.Timestamp64.AddDays(largeDays))
            .SingleAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTime Timestamp64 { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DateTimeArithmeticEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
                entity.Property(e => e.Timestamp).HasColumnType("Timestamp");
                entity.Property(e => e.Timestamp64).HasColumnType("Timestamp64");
            });
    }
}
