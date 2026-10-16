using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateOnlyFromDayNumberYdbTest
{
    [Fact]
    public async Task FromDayNumber_matches_date_columns()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateOnlyFromDayNumberYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 1, DayNumber = 0, Date = new DateOnly(1, 1, 1) },
            new TestEntity { Id = 2, DayNumber = 726780, Date = new DateOnly(1990, 11, 10) });
        await context.SaveChangesAsync();

        Assert.Equal([1, 2], await context.Entities
            .Where(e => DateOnly.FromDayNumber(e.DayNumber) == e.Date)
            .OrderBy(e => e.Id)
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public int DayNumber { get; set; }
        public DateOnly Date { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DateOnlyFromDayNumberEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
                entity.Property(e => e.Date).HasColumnType("Date32");
            });
    }
}
