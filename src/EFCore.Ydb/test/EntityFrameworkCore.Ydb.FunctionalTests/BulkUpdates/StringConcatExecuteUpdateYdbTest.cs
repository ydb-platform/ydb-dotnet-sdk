using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class StringConcatExecuteUpdateYdbTest
{
    [Fact]
    public async Task ExecuteUpdate_with_string_concatenation()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(StringConcatExecuteUpdateYdbTest));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new TestContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Entities.Add(new TestEntity { Id = 1, Name = "Monty" });
        await context.SaveChangesAsync();

        await context.Entities
            .Where(e => e.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Name, e => e.Name + "Modified"));

        ((TestSqlLoggerFactory)sqlLoggerFactory).AssertBaseline([
            """
            UPDATE `TestEntities`
            SET `Name` = `Name` || 'Modified'u
            WHERE `Id` = 1
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
