using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class StringStartsWithYdbTest
{
    [Fact]
    public async Task StartsWith_column_pattern_matches_prefix()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(StringStartsWithYdbTest));
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 1, Name = "alpha", Prefix = "al" },
            new TestEntity { Id = 2, Name = "xalpha", Prefix = "al" },
            new TestEntity { Id = 3, Name = "alpha", Prefix = "" },
            new TestEntity { Id = 4, Prefix = "al" },
            new TestEntity { Id = 5, Name = "alpha" },
            new TestEntity { Id = 6, Name = "b%tail", Prefix = "b%" },
            new TestEntity { Id = 7, Name = "ab%tail", Prefix = "b%" },
            new TestEntity { Id = 8, Name = "tailb%", Prefix = "b%" });
        await context.SaveChangesAsync();

        Assert.Equal([1, 3, 6], await context.Entities
            .Where(e => e.Name!.StartsWith(e.Prefix!))
            .OrderBy(e => e.Id)
            .Select(e => e.Id)
            .ToListAsync());

        Assert.Equal([3, 8], await context.Entities
            .Where(e => e.Name!.EndsWith(e.Prefix!))
            .OrderBy(e => e.Id)
            .Select(e => e.Id)
            .ToListAsync());
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? Prefix { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) =>
            model.Entity<TestEntity>(entity =>
            {
                entity.ToTable("StringStartsWithEntities");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).ValueGeneratedNever();
            });
    }
}
