using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class SkipTakeYdbTest
{
    [Fact]
    public async Task Skip_and_Take_returns_expected_page()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(SkipTakeYdbTest));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new TestContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Entities.AddRange(
            new TestEntity { Id = 1, Name = "a" },
            new TestEntity { Id = 2, Name = "b" },
            new TestEntity { Id = 3, Name = "c" });
        await context.SaveChangesAsync();

        ((TestSqlLoggerFactory)sqlLoggerFactory).Clear();

        var result = await context.Entities
            .OrderBy(e => e.Id)
            .Skip(1)
            .Take(1)
            .Select(e => e.Name)
            .ToListAsync();

        Assert.Equal(["b"], result);

        var sql = Assert.Single(((TestSqlLoggerFactory)sqlLoggerFactory).SqlStatements);
        Assert.Contains("LIMIT", sql);
        Assert.Contains("OFFSET", sql);
    }

    [Fact]
    public async Task Skip_with_large_Take_returns_remaining_rows()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(SkipTakeYdbTest) + "_offset_only");
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new TestContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Entities.AddRange(
            new TestEntity { Id = 1, Name = "a" },
            new TestEntity { Id = 2, Name = "b" },
            new TestEntity { Id = 3, Name = "c" });
        await context.SaveChangesAsync();

        ((TestSqlLoggerFactory)sqlLoggerFactory).Clear();

        var result = await context.Entities
            .OrderBy(e => e.Id)
            .Skip(1)
            .Take(int.MaxValue)
            .Select(e => e.Name)
            .ToListAsync();

        Assert.Equal(["b", "c"], result);

        var sql = Assert.Single(((TestSqlLoggerFactory)sqlLoggerFactory).SqlStatements);
        Assert.Contains("LIMIT", sql);
        Assert.Contains("OFFSET", sql);
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class TestContext(ListLoggerFactory sqlLoggerFactory) : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<TestEntity>(b =>
            {
                b.ToTable("TestEntities");
                b.HasKey(e => e.Id);
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .UseLoggerFactory(sqlLoggerFactory)
            .EnableServiceProviderCaching(false);
    }
}
