using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateOnlyFromDateTimeYdbTest
{
    [Fact]
    public async Task FromDateTime_matches_date_constants()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateOnlyFromDateTimeYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.Add(new TestEntity
        {
            Id = 1,
            Timestamp = new DateTime(1998, 5, 4, 15, 30, 10, DateTimeKind.Utc),
            WideTimestamp = new DateTime(1, 10, 11, 23, 59, 59, DateTimeKind.Utc)
        });
        await context.SaveChangesAsync();

        Assert.Equal([1], await context.Entities
            .Where(e => DateOnly.FromDateTime(e.Timestamp) == new DateOnly(1998, 5, 4)
                        && DateOnly.FromDateTime(e.WideTimestamp) == new DateOnly(1, 10, 11))
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTime WideTimestamp { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DateOnlyFromDateTimeEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
                entity.Property(e => e.WideTimestamp).HasColumnType("Timestamp64");
            });
    }
}
