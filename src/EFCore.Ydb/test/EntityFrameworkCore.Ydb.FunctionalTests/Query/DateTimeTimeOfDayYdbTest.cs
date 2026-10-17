using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateTimeTimeOfDayYdbTest
{
    [Fact]
    public async Task TimeOfDay_matches_fractional_time()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateTimeTimeOfDayYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 1, DateTime = new DateTime(1990, 11, 10) },
            new TestEntity { Id = 2, DateTime = new DateTime(1990, 11, 10, 1, 2, 3).AddTicks(4567890) });
        await context.SaveChangesAsync();

        var time = new TimeSpan(0, 1, 2, 3).Add(TimeSpan.FromTicks(4567890));

        Assert.Equal([2], await context.Entities
            .Where(e => e.DateTime.TimeOfDay == time)
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateTime DateTime { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DateTimeTimeOfDayEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
            });
    }
}
