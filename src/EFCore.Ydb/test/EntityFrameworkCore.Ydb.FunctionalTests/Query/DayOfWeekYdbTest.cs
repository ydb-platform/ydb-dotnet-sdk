using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DayOfWeekYdbTest
{
    [Fact]
    public async Task DayOfWeek_matches_dotnet_for_all_weekdays()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DayOfWeekYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        var sunday = new DateTime(2024, 5, 5, 12, 34, 56, DateTimeKind.Utc);
        context.Entities.AddRange(Enumerable.Range(0, 7).Select(day => new TestEntity
        {
            Id = day + 1,
            Timestamp = sunday.AddDays(day),
            Date = DateOnly.FromDateTime(sunday.AddDays(day))
        }));
        await context.SaveChangesAsync();

        Assert.Equal([0, 1, 2, 3, 4, 5, 6], await context.Entities
            .OrderBy(e => e.Id)
            .Select(e => (int)e.Timestamp.DayOfWeek)
            .ToListAsync());

        Assert.Equal([0, 1, 2, 3, 4, 5, 6], await context.Entities
            .OrderBy(e => e.Id)
            .Select(e => (int)e.Date.DayOfWeek)
            .ToListAsync());

        Assert.Equal([1], await context.Entities
            .Where(e => e.Timestamp.DayOfWeek == DayOfWeek.Sunday)
            .Select(e => e.Id)
            .ToListAsync());

        Assert.Equal([1], await context.Entities
            .Where(e => e.Date.DayOfWeek == DayOfWeek.Sunday)
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
        public DateOnly Date { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model)
            => model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DayOfWeekEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
            });
    }
}
