using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class DateTimeExecuteUpdateYdbTest
{
    [Fact]
    public async Task ExecuteUpdate_with_DateTime_UtcNow()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("DateTimeExecuteUpdateYdbTest");
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new TestContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Entities.Add(new TestEntity { Id = 1, Name = "Test", UpdatedAt = DateTime.UtcNow });
        await context.SaveChangesAsync();

        await context.Entities
            .Where(e => e.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.UpdatedAt, DateTime.UtcNow));

        ((TestSqlLoggerFactory)sqlLoggerFactory).AssertBaseline([
            """
            UPDATE `TestEntities`
            SET `UpdatedAt` = CurrentUtcTimestamp()
            WHERE `Id` = 1
            """
        ], false);
    }

    [Fact]
    public async Task ExecuteUpdate_with_constant_DateTime()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("DateTimeExecuteUpdateYdbTest");
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new TestContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Entities.Add(new TestEntity { Id = 1, Name = "Test", UpdatedAt = DateTime.UtcNow });
        await context.SaveChangesAsync();

        var testDate = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc);

        await context.Entities
            .Where(e => e.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.UpdatedAt, testDate));
        ((TestSqlLoggerFactory)sqlLoggerFactory).AssertBaseline([
            """
            $__testDate_0='?' (DbType = Object)

            UPDATE `TestEntities`
            SET `UpdatedAt` = @__testDate_0
            WHERE `Id` = 1
            """
        ], false);

        var entity = await context.Entities.AsNoTracking().FirstOrDefaultAsync(e => e.Id == 1);
        Assert.NotNull(entity);
        Assert.Equal(testDate, entity.UpdatedAt);
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public DateTime UpdatedAt { get; set; }
    }

    public class TestContext(ListLoggerFactory sqlLoggerFactory) : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<TestEntity>(b =>
            {
                b.ToTable("TestEntities");
                b.HasKey(e => e.Id);
                b.Property(e => e.UpdatedAt).HasColumnType("Timestamp");
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .UseLoggerFactory(sqlLoggerFactory)
            .EnableServiceProviderCaching(false);
    }
}
