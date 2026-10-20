using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DateOnlyToDateTimeYdbTest
{
    [Fact]
    public void ToDateTime_with_string_converted_TimeOnly_is_not_translated()
    {
        using var context = new TestContext();

        Assert.Throws<InvalidOperationException>(() => context.Entities
            .Where(e => e.Date.ToDateTime(e.Time) == new DateTime(2020, 1, 1, 15, 30, 10))
            .ToQueryString());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public DateOnly Date { get; set; }
        public TimeOnly Time { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.Property(e => e.Date).HasColumnType("Date32");
                entity.Property(e => e.Time).HasConversion<string>();
            });
    }
}
