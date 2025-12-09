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

    [Fact]
    public async Task InsertBatchTest()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest");
        await using var dbContext = new TestEntityDbContext(_assertSql);
        await testStore.CleanAsync(dbContext);

        await dbContext.Database.EnsureCreatedAsync();
        var order1 = new Order { CreatedAt = DateTime.Now };
        var order2 = new Order { CreatedAt = DateTime.Now };
        var order3 = new Order { Id = -1, CreatedAt = DateTime.Now, Discount = 20 };
        var order4 = new Order { Id = -2, CreatedAt = DateTime.Now, Discount = 30 };
        var order5 = new Order { CreatedAt = DateTime.Now, Discount = 20 };
        var order6 = new Order { CreatedAt = DateTime.Now, Discount = 30 };

        dbContext.Orders.Add(order1);
        dbContext.Orders.Add(order2);
        dbContext.Orders.Add(order3);
        dbContext.Orders.Add(order4);
        dbContext.Orders.Add(order5);
        dbContext.Orders.Add(order6);
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

        Assert.Equal(1, order1.Id);
        Assert.Equal(2, order2.Id);
        Assert.Equal(-1, order3.Id);
        Assert.Equal(-2, order4.Id);
        Assert.Equal(3, order5.Id);
        Assert.Equal(4, order6.Id);

        Assert.Equal(0, order1.Discount);
        Assert.Equal(0, order2.Discount);
        Assert.Equal(20, order3.Discount);
        Assert.Equal(30, order4.Discount);
        Assert.Equal(20, order5.Discount);
        Assert.Equal(30, order6.Discount);
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
