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
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest_Insert");
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
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest_Delete");
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

    [Fact]
    public async Task UpdateBatchTest()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest_Update");
        await using var dbContext = new TestEntityDbContext(_assertSql);
        await testStore.CleanAsync(dbContext);
        await dbContext.Database.EnsureCreatedAsync();

        dbContext.Orders.AddRange(_order1, _order2, _order3, _order4, _order5, _order6);
        await dbContext.SaveChangesAsync();

        _order1.Price = 0;
        _order2.Price = 1;
        _order3.Price = 2;
        _order4.Price = 3;
        _order4.Status = "New_New";
        _order5.Price = 4;
        _order5.Status = "New_New";
        _order6.Price = 5;
        _order6.Status = "New_New";
        await dbContext.SaveChangesAsync();

        AssertSql(
            """
            $p2='?' (DbType = Int32)
            $p0='?' (DbType = Decimal)
            $p1='?'
            $batch_value_0='?' (DbType = Object)
            $batch_value_1='?' (DbType = Object)

            UPDATE `Order` SET `Price` = @p0, `Status` = @p1
            WHERE `Id` = @p2;
            UPDATE `Order` ON SELECT * FROM AS_TABLE($batch_value_0);
            UPDATE `Order` ON SELECT * FROM AS_TABLE($batch_value_1);
            """);

        Assert.Equal(3, await dbContext.Orders.CountAsync(o => o.Status == "New_New"));
    }

    [Fact]
    public async Task MixedBatchTest()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest_Mixed");
        await using var dbContext = new TestEntityDbContext(_assertSql);
        await testStore.CleanAsync(dbContext);
        await dbContext.Database.EnsureCreatedAsync();

        var order1 = new Order { CreatedAt = DateTime.Now };
        var order2 = new Order { CreatedAt = DateTime.Now, Discount = 10 };
        var order3 = new Order { CreatedAt = DateTime.Now, Discount = 20 };

        var customer1 = new Customer { Name = "Alice", IsVip = false, CreatedAt = DateTime.Now };
        var customer2 = new Customer { Name = "Bob", IsVip = true, CreatedAt = DateTime.Now };
        var customer3 = new Customer { Name = "Eve", IsVip = false, CreatedAt = DateTime.Now };

        dbContext.AddRange(_order1, _order2, order1, order2, order3, customer1, customer2, customer3);
        await dbContext.SaveChangesAsync();

        AssertSql(
            """
            $batch_value_0='?' (DbType = Object)
            $batch_value_1='?' (DbType = Object)
            $batch_value_2='?' (DbType = Object)

            INSERT INTO `Customers` SELECT * FROM AS_TABLE($batch_value_0)
            RETURNING `Id`;
            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_1)
            RETURNING `Id`, `Discount`;
            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_2)
            RETURNING `Id`;
            """);
        var order4 = new Order { CreatedAt = DateTime.Now, Discount = 30 };
        var order5 = new Order { CreatedAt = DateTime.Now, Discount = 40 };
        dbContext.Orders.AddRange(order4, order5);
        dbContext.Orders.RemoveRange(_order1, _order2, order3);

        var customer4 = new Customer { Name = "Dave", IsVip = false, CreatedAt = DateTime.Now };
        var customer5 = new Customer { Name = "Kirill", IsVip = true, CreatedAt = DateTime.Now };
        dbContext.Customers.AddRange(customer4, customer5);

        order1.Price = 100;
        order1.Status = "Processed";
        order2.Price = 200;
        order2.Status = "Processed";

        customer1.IsVip = true;
        customer2.IsVip = false;

        dbContext.Customers.Remove(customer3);

        await dbContext.SaveChangesAsync();

        AssertSql(
            """
            $p0='?' (DbType = Int32)
            $batch_value_0='?' (DbType = Object)
            $batch_value_1='?' (DbType = Object)
            $batch_value_2='?' (DbType = Object)
            $batch_value_3='?' (DbType = Object)
            $p1='?' (DbType = Int32)
            $batch_value_4='?' (DbType = Object)

            DELETE FROM `Customers`
            WHERE `Id` = @p0;
            UPDATE `Customers` ON SELECT * FROM AS_TABLE($batch_value_0);
            DELETE FROM `Order` ON SELECT * FROM AS_TABLE($batch_value_1);
            UPDATE `Order` ON SELECT * FROM AS_TABLE($batch_value_2);
            INSERT INTO `Customers` SELECT * FROM AS_TABLE($batch_value_3)
            RETURNING `Id`;
            DELETE FROM `Order`
            WHERE `Id` = @p1;
            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_4)
            RETURNING `Id`;
            """);

        Assert.Equal(100m, order1.Price);
        Assert.Equal("Processed", order1.Status);
        Assert.Equal(200m, order2.Price);
        Assert.Equal("Processed", order2.Status);
        Assert.Equal(2, await dbContext.Orders.CountAsync(o => o.Status == "Processed"));
        Assert.False(
            await dbContext.Orders.AnyAsync(o => o.Id == order3.Id || o.Id == _order1.Id || o.Id == _order2.Id));
        Assert.Equal(30m, order4.Discount);
        Assert.Equal(40m, order5.Discount);
        Assert.True(customer1.IsVip);
        Assert.False(customer2.IsVip);
        Assert.Equal(2, await dbContext.Customers.CountAsync(o => o.IsVip));
        Assert.False(await dbContext.Customers.AnyAsync(c => c.Id == customer3.Id));
        Assert.Equal("Dave", customer4.Name);
    }

    [Fact]
    public async Task Insert15000BatchTest()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("YdbModificationCommandBatchTest_Stress");
        await using var dbContext = new TestEntityDbContext(_assertSql);
        await testStore.CleanAsync(dbContext);
        await dbContext.Database.EnsureCreatedAsync();

        const int initialOrders = 7500;
        const int initialCustomers = 7500;

        for (var i = 0; i < initialOrders; i++)
        {
            dbContext.Orders.Add(new Order { CreatedAt = DateTime.UtcNow, Price = i, Discount = i % 2 == 0 ? 10 : 0 });
        }

        for (var i = 0; i < initialCustomers; i++)
        {
            dbContext.Customers.Add(new Customer
                { Name = "Customer_" + i, IsVip = i % 10 == 0, CreatedAt = DateTime.UtcNow });
        }

        await dbContext.SaveChangesAsync();
        AssertSql(
            """
            $batch_value_0='?' (DbType = Object)
            $batch_value_1='?' (DbType = Object)

            INSERT INTO `Customers` SELECT * FROM AS_TABLE($batch_value_0)
            RETURNING `Id`;
            INSERT INTO `Order` SELECT * FROM AS_TABLE($batch_value_1)
            RETURNING `Id`;
            """);
        Assert.Equal(initialOrders, await dbContext.Orders.CountAsync());
        Assert.Equal(initialCustomers, await dbContext.Customers.CountAsync());
    }

    private void AssertSql(params string[] expected) => _assertSql.AssertBaseline(expected, false);

    public class TestEntityDbContext(ILoggerFactory loggerFactory) : DbContext
    {
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Customer> Customers => Set<Customer>();

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

    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public bool IsVip { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}
