using System;
using System.Linq;
using System.Threading.Tasks;
using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

/// <summary>
/// Test for DateTime handling in ExecuteUpdate operations.
/// This test verifies the fix for the issue where DateTime.UtcNow was generating invalid SQL.
/// </summary>
public class DateTimeBulkUpdatesYdbTest
{
    [Fact]
    public async Task ExecuteUpdate_with_DateTime_UtcNow()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("DateTimeBulkUpdatesTest");
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.MigrateAsync();
        
        // Add a test entity
        context.Entities.Add(new TestEntity { Id = 1, Name = "Test", UpdatedAt = DateTime.UtcNow });
        await context.SaveChangesAsync();
        
        // This test verifies that DateTime.UtcNow is correctly translated to CurrentUtcDateTime function
        // Previously, this would fail with error: "Datetime() requires exactly 1 arguments, given: 0"
        var rowsAffected = await context.Entities
            .Where(e => e.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.UpdatedAt, DateTime.UtcNow));
        
        Assert.Equal(1, rowsAffected);
    }

    [Fact]
    public async Task ExecuteUpdate_with_constant_DateTime()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("DateTimeBulkUpdatesTest2");
        await using var context = new TestContext();
        await testStore.CleanAsync(context);
        await context.Database.MigrateAsync();
        
        // Add a test entity
        context.Entities.Add(new TestEntity { Id = 1, Name = "Test", UpdatedAt = DateTime.UtcNow });
        await context.SaveChangesAsync();
        
        var testDate = new DateTime(2023, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        
        var rowsAffected = await context.Entities
            .Where(e => e.Id == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.UpdatedAt, testDate));
        
        Assert.Equal(1, rowsAffected);
        
        // Verify the value was updated
        var entity = await context.Entities.FirstOrDefaultAsync(e => e.Id == 1);
        Assert.NotNull(entity);
        Assert.Equal(testDate, entity.UpdatedAt);
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public DateTime UpdatedAt { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntity>(b =>
            {
                b.ToTable("TestEntities");
                b.HasKey(e => e.Id);
                b.Property(e => e.UpdatedAt).HasColumnType("Timestamp");
            });
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136", builder => builder.DisableRetryOnFailure())
            .EnableServiceProviderCaching(false);
    }
}
