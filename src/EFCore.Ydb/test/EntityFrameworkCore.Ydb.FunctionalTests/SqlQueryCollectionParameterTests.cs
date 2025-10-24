using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class SqlQueryCollectionParameterTests
{
    [Fact]
    public async Task SqlQuery_DoesNotExpandCollectionParameter_InClause()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("SqlQueryCollectionParameterTests");
        await using var testDbContext = new TestDbContext();
        await testStore.CleanAsync(testDbContext);
        await testDbContext.Database.EnsureCreatedAsync();

        var ids = new List<int> { 1, 2, 3 };
        testDbContext.Items.AddRange(new TestEntity { Id = 1, Price = 1 }, new TestEntity { Id = 2, Price = 2 });
        await testDbContext.SaveChangesAsync();
        
        var rows = await testDbContext.Database.SqlQuery<TestEntity>(
            $"SELECT * FROM TestEntity WHERE Id = ({ids})").ToListAsync();
        
        Assert.Equal(2, rows.Count);
    }

    public sealed class TestDbContext : DbContext
    {
        public DbSet<TestEntity> Items => Set<TestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.Entity<TestEntity>(b =>
        {
            b.ToTable("TestEntity");
            b.HasKey(x => x.Id);
        });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
                .UseYdb("Host=localhost;Port=2136")
                .EnableServiceProviderCaching(false);
    }

    public sealed class TestEntity
    {
        public int Id { get; init; }
        public int Price { get; init; }
    }
}
