using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateOnlyArithmeticYdbTest
{
    [Fact]
    public async Task DateOnly_arithmetic_matches_date_constants()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateOnlyArithmeticYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.Add(new TestEntity { Id = 1, Date = new DateOnly(1990, 11, 10) });
        await context.SaveChangesAsync();

        Assert.Equal([1], await context.Entities
            .Where(e => e.Date.AddYears(3) == new DateOnly(1993, 11, 10))
            .Select(e => e.Id)
            .ToListAsync());
        Assert.Equal([1], await context.Entities
            .Where(e => e.Date.AddMonths(3) == new DateOnly(1991, 2, 10))
            .Select(e => e.Id)
            .ToListAsync());
        Assert.Equal([1], await context.Entities
            .Where(e => e.Date.AddDays(3) == new DateOnly(1990, 11, 13))
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
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
                entity.ToTable("DateOnlyArithmeticEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
            });
    }
}
