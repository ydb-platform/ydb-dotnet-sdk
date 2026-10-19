using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateOnlyDayNumberYdbTest
{
    [Fact]
    public async Task DayNumber_matches_date_constants()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateOnlyDayNumberYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.Add(new TestEntity
        {
            Id = 1,
            Date = new DateOnly(1990, 11, 10),
            WideDate = new DateOnly(1, 1, 1)
        });
        await context.SaveChangesAsync();

        Assert.Equal([1], await context.Entities
            .Where(e => e.Date.DayNumber == 726780 && e.WideDate.DayNumber == 0)
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateOnly Date { get; set; }
        public DateOnly WideDate { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DateOnlyDayNumberEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
                entity.Property(e => e.WideDate).HasColumnType("Date32");
            });
    }
}
