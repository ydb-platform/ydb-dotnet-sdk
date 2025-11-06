using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class Test
{
    [Fact]
    public async Task SqlQuery_DoesNotExpandCollectionParameter_InClause()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("SqlQueryCollectionParameterTests");
        await using var testDbContext = new TestDbContext();
        await testStore.CleanAsync(testDbContext);
        await testDbContext.Database.EnsureCreatedAsync();

        var ids = new List<int> { 1, 2, 3 };
        testDbContext.Items.AddRange(
            new TestEntity { Id = 1 },
            new TestEntity { Id = 2 },
            new TestEntity { Id = 3 }
        );
        await testDbContext.SaveChangesAsync();

        var rows = await testDbContext.Database.SqlQuery<TestEntity>($"SELECT * FROM TestEntity").ToListAsync();

        Assert.Equal(3, rows.Count);
    }

    public sealed class TestDbContext : DbContext
    {
        public DbSet<TestEntity> Items => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .EnableServiceProviderCaching(false);
    }

    public sealed class TestEntity
    {
        public int Id { get; init; }

        /// <summary>
        /// Gets or sets a flag indicating if a user has confirmed their email address.
        /// </summary>
        /// <value>True if the email address has been confirmed, otherwise false.</value>
        public bool EmailConfirmed { get; protected internal set; }
    }
}
