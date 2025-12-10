using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class YdbModificationCommandBatchTest
{
    private readonly TestSqlLoggerFactory _assertSql = new();
    private readonly Order _order1 = new() { CreatedAt = DateTime.Now };
    private readonly Order _order2 = new() { CreatedAt = DateTime.Now };
    private readonly Order _order3 = new() { Id = -1, CreatedAt = DateTime.Now, Discount = 20 };
    private readonly Order _order4 = new() { Id = -2, CreatedAt = DateTime.Now, Discount = 30 };
    private readonly Order _order5 = new() { CreatedAt = DateTime.Now, Discount = 20 };
    private readonly Order _order6 = new() { CreatedAt = DateTime.Now, Discount = 30 };

    [Fact]
    public async Task InsertBatchTest()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest");
        await using var dbContext = new TestEntityDbContext(_assertSql);
        await testStore.CleanAsync(dbContext);
        await dbContext.Database.EnsureCreatedAsync();

        dbContext.Orders.AddRange(_order1, _order2, _order3, _order4, _order5, _order6);
        await dbContext.SaveChangesAsync();
        AssertSql(
            """
            $batch_value_0='?' (DbType = Object)
            $batch_value_1='?' (DbType = Object)
            $batch_value_2='?' (DbType = Object)

            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_0)
            RETURNING `Id`, `Discount`;
            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_1)
            RETURNING `Id`;
            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_2);
            """);

        Assert.Equal(1, _order1.Id);
        Assert.Equal(2, _order2.Id);
        Assert.Equal(-1, _order3.Id);
        Assert.Equal(-2, _order4.Id);
        Assert.Equal(3, _order5.Id);
        Assert.Equal(4, _order6.Id);

        Assert.Equal(0, _order1.Discount);
        Assert.Equal(0, _order2.Discount);
        Assert.Equal(20, _order3.Discount);
        Assert.Equal(30, _order4.Discount);
        Assert.Equal(20, _order5.Discount);
        Assert.Equal(30, _order6.Discount);
    }

    [Fact]
    public async Task DeleteBatchTest()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest");
        await using var dbContext = new TestEntityDbContext(_assertSql);
        await testStore.CleanAsync(dbContext);
        await dbContext.Database.EnsureCreatedAsync();

        dbContext.Orders.AddRange(_order1, _order2, _order3, _order4, _order5, _order6);
        await dbContext.SaveChangesAsync();
        dbContext.Orders.RemoveRange(_order1, _order2, _order3, _order4, _order5, _order6);
        await dbContext.SaveChangesAsync();

        AssertSql(
            """
            $batch_value_0='?' (DbType = Object)

            DELETE FROM `Order` ON SELECT * FROM AS_TABLE($batch_value_0);
            """);

        Assert.Empty(dbContext.Orders);
    }

    private void AssertSql(params string[] expected) => _assertSql.AssertBaseline(expected, false);

    public class TestEntityDbContext(ILoggerFactory loggerFactory) : DbContext
    {
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<Order>(entity =>
            {
                entity.ToTable("Order");
                entity.Property(e => e.Discount)
                    .HasPrecision(18, 2)
                    .HasDefaultValue(0m);
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseYdb("Host=localhost;Port=2136")
                .UseLoggerFactory(loggerFactory);
    }

    public class Order
    {
        public int Id { get; set; }

        public decimal Price { get; set; }

        public decimal? Discount { get; set; }

        public DateTime CreatedAt { get; set; }

        public string Status { get; set; } = "New";
    }
}
