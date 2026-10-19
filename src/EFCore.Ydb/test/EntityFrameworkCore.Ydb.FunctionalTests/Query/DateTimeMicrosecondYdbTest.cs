using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateTimeMicrosecondYdbTest
{
    [Fact]
    public async Task Microsecond_matches_sub_millisecond_component()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(DateTimeMicrosecondYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity
            {
                Id = 1,
                Timestamp = new DateTime(2000, 2, 29, 1, 2, 3).AddTicks(1234560),
                Timestamp64 = new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(1234560),
                OptionalTimestamp64 = new DateTime(1968, 2, 29, 1, 2, 3).AddTicks(1234560)
            },
            new TestEntity
            {
                Id = 2,
                Timestamp = DateTime.UnixEpoch,
                Timestamp64 = DateTime.UnixEpoch.AddTicks(-10)
            });
        await context.SaveChangesAsync();

        var component = 456;
        Assert.Equal([1], await context.Entities
            .Where(e => e.Timestamp.Microsecond == component
                        && e.Timestamp64.Microsecond == component
                        && e.OptionalTimestamp64!.Value.Microsecond == component)
            .Select(e => e.Id)
            .ToListAsync());

        var result = await context.Entities.OrderBy(e => e.Id).Select(e => new
        {
            Timestamp = e.Timestamp.Microsecond,
            Timestamp64 = e.Timestamp64.Microsecond,
            OptionalTimestamp64 = (int?)e.OptionalTimestamp64!.Value.Microsecond,
            e.Timestamp.Millisecond
        }).ToListAsync();

        Assert.Equal([456, 0], result.Select(e => e.Timestamp));
        Assert.Equal([456, 999], result.Select(e => e.Timestamp64));
        Assert.Equal([456, null], result.Select(e => e.OptionalTimestamp64));
        Assert.Equal([123, 0], result.Select(e => e.Millisecond));
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
                entity.ToTable("DateTimeMicrosecondEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
                entity.Property(e => e.Timestamp64).HasColumnType("Timestamp64");
                entity.Property(e => e.OptionalTimestamp64).HasColumnType("Timestamp64");
            });
    }
}
