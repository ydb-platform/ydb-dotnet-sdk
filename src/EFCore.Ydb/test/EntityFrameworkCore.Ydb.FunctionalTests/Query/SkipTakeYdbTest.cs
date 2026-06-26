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

        ((TestSqlLoggerFactory)sqlLoggerFactory).AssertBaseline([
            """
            $__p_0='?' (DbType = Int32)

            SELECT `t`.`Name`
            FROM `TestEntities` AS `t`
            ORDER BY `t`.`Id`
            LIMIT @__p_0 OFFSET @__p_0
            """
        ], false);
    }

    [Fact]
    public async Task Skip_without_Take_uses_default_limit()
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
            .Select(e => e.Name)
            .ToListAsync();

        Assert.Equal(["b", "c"], result);

        ((TestSqlLoggerFactory)sqlLoggerFactory).AssertBaseline([
            """
            $__p_0='?' (DbType = Int32)

            SELECT `t`.`Name`
            FROM `TestEntities` AS `t`
            ORDER BY `t`.`Id`
            LIMIT 2147483647 OFFSET @__p_0
            """
        ], false);
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
