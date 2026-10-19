using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateTimeDateYdbTest
{
    [Fact]
    public async Task Date_preserves_timestamp_mapping_and_pre_epoch_values()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateTimeDateYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity
            {
                Id = 1,
                Timestamp = new DateTime(2000, 2, 29, 1, 2, 3).AddTicks(4567890),
                Timestamp64 = new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(4567890),
                OptionalTimestamp64 = new DateTime(1968, 2, 29, 23, 59, 59).AddTicks(9999990)
            },
            new TestEntity
            {
                Id = 2,
                Timestamp = new DateTime(2000, 3, 1),
                Timestamp64 = new DateTime(1969, 12, 31, 23, 59, 59).AddTicks(9999990)
            });
        await context.SaveChangesAsync();

        var date = new DateTime(1968, 2, 29);
        var timestampDate = new DateTime(2000, 2, 29);

        Assert.Equal([1], await context.Entities
            .Where(e => e.Timestamp64.Date == date
                        && e.Timestamp.Date == timestampDate
                        && e.OptionalTimestamp64!.Value.Date == date)
            .Select(e => e.Id)
            .ToListAsync());

        var result = await context.Entities.OrderBy(e => e.Id).Select(e => new
        {
            Timestamp = e.Timestamp.Date,
            Timestamp64 = e.Timestamp64.Date,
            OptionalTimestamp64 = (DateTime?)e.OptionalTimestamp64!.Value.Date
        }).ToListAsync();

        Assert.Equal([timestampDate, new DateTime(2000, 3, 1)], result.Select(e => e.Timestamp));
        Assert.Equal([date, new DateTime(1969, 12, 31)], result.Select(e => e.Timestamp64));
        Assert.Equal([date, null], result.Select(e => e.OptionalTimestamp64));
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTime Timestamp64 { get; set; }
        public DateTime? OptionalTimestamp64 { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("DateTimeDateEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
                entity.Property(e => e.Timestamp64).HasColumnType("Timestamp64");
                entity.Property(e => e.OptionalTimestamp64).HasColumnType("Timestamp64");
            });
    }
}
