using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class CorrelatedExecuteUpdateYdbTest
{
    [Fact]
    public async Task Update_without_correlation_uses_base_sql()
    {
        await using var testStore = CreateStore(nameof(Update_without_correlation_uses_base_sql));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedUpdateContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        context.Orders.Add(new Order { Id = 1, CustomerId = 1, Status = "Pending" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Orders
            .Where(order => order.Id == 1)
            .ExecuteUpdateAsync(setters => setters.SetProperty(order => order.Status, "Shipped"));

#if EFCORE9
        AssertSql(logger, """
                          UPDATE `Orders`
                          SET `Status` = 'Shipped'u
                          WHERE `Id` = 1
                          """);
#else
        AssertSql(logger, """
                          $p='?'

                          UPDATE `Orders`
                          SET `Status` = @p
                          WHERE `Id` = 1
                          """);
#endif
        logger.Clear();
        Assert.Equal("Shipped", await context.Orders.AsNoTracking().Select(order => order.Status).SingleAsync());
    }

    [Fact]
    public async Task Update_with_distinct_without_correlation_uses_base_sql()
    {
        await using var testStore = CreateStore(nameof(Update_with_distinct_without_correlation_uses_base_sql));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedUpdateContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        context.Orders.Add(new Order { Id = 1, CustomerId = 1, Status = "Pending" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Orders
            .Where(order => order.Id == 1)
            .Distinct()
            .ExecuteUpdateAsync(setters => setters.SetProperty(order => order.Status, "Shipped"));

#if EFCORE9
        AssertSql(logger, """
                          UPDATE `Orders`
                          SET `Status` = 'Shipped'u
                          WHERE `Id` = 1
                          """);
#else
        AssertSql(logger, """
                          $p='?'

                          UPDATE `Orders`
                          SET `Status` = @p
                          WHERE `Id` = 1
                          """);
#endif
        logger.Clear();
        Assert.Equal("Shipped", await context.Orders.AsNoTracking().Select(order => order.Status).SingleAsync());
    }

    [Fact]
    public async Task Update_with_navigation_uses_update_on_instead_of_correlated_subquery()
    {
        await using var testStore =
            CreateStore(nameof(Update_with_navigation_uses_update_on_instead_of_correlated_subquery));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedUpdateContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Customers.AddRange(
            new Customer { Id = 1, Name = "Acme" },
            new Customer { Id = 2, Name = "Keep" });
        context.Orders.AddRange(
            new Order { Id = 10, CustomerId = 1, Status = "Pending" },
            new Order { Id = 20, CustomerId = 2, Status = "Pending" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Orders
            .Where(order => order.Customer!.Name == "Acme")
            .ExecuteUpdateAsync(setters => setters.SetProperty(order => order.Status, "Shipped"));

#if EFCORE9
        AssertSql(logger, """
                          UPDATE `Orders` ON SELECT `o`.`Id` AS `Id`, 'Shipped'u AS `Status`
                          FROM `Orders` AS `o`
                          INNER JOIN `Customers` AS `c` ON `o`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
#else
        AssertSql(logger, """
                          $p='?'

                          UPDATE `Orders` ON SELECT `o`.`Id` AS `Id`, @p AS `Status`
                          FROM `Orders` AS `o`
                          INNER JOIN `Customers` AS `c` ON `o`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
#endif
        Assert.DoesNotContain("EXISTS", logger.SqlStatements[0]);
        logger.Clear();
        Assert.Equal(
            ["Shipped", "Pending"],
            await context.Orders.AsNoTracking().OrderBy(order => order.Id).Select(order => order.Status).ToListAsync());
    }

    [Fact]
    public async Task Update_with_navigation_projects_every_composite_key_column()
    {
        await using var testStore = CreateStore(nameof(Update_with_navigation_projects_every_composite_key_column));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedUpdateContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        context.Orders.Add(new Order { Id = 10, CustomerId = 1, Status = "Pending" });
        context.OrderLines.AddRange(
            new OrderLine { OrderId = 10, LineId = 1, Product = "Old" },
            new OrderLine { OrderId = 10, LineId = 2, Product = "Old" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.OrderLines
            .Where(line => line.Order!.Customer!.Name == "Acme")
            .ExecuteUpdateAsync(setters => setters.SetProperty(line => line.Product, "Updated"));

#if EFCORE9
        AssertSql(logger, """
                          UPDATE `OrderLines` ON SELECT `o`.`OrderKey` AS `OrderKey`, `o`.`LineKey` AS `LineKey`, 'Updated'u AS `Product`
                          FROM `OrderLines` AS `o`
                          INNER JOIN `Orders` AS `o0` ON `o`.`OrderKey` = `o0`.`Id`
                          INNER JOIN `Customers` AS `c` ON `o0`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
#else
        AssertSql(logger, """
                          $p='?'

                          UPDATE `OrderLines` ON SELECT `o`.`OrderKey` AS `OrderKey`, `o`.`LineKey` AS `LineKey`, @p AS `Product`
                          FROM `OrderLines` AS `o`
                          INNER JOIN `Orders` AS `o0` ON `o`.`OrderKey` = `o0`.`Id`
                          INNER JOIN `Customers` AS `c` ON `o0`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
#endif
        logger.Clear();
        Assert.Equal(
            ["Updated", "Updated"],
            await context.OrderLines.AsNoTracking().OrderBy(line => line.LineId).Select(line => line.Product)
                .ToListAsync());
    }

    private static async Task InitializeAsync(TestStore testStore, DbContext context)
    {
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
    }

    private static TestStore CreateStore(string testName)
        => YdbTestStoreFactory.Instance.Create($"{nameof(CorrelatedExecuteUpdateYdbTest)}_{testName}");

    private static void AssertSql(TestSqlLoggerFactory logger, string expected)
    {
        if (logger.SqlStatements.Count == 1 && logger.SqlStatements[0] == expected)
        {
            return;
        }

        logger.AssertBaseline([expected], assertOrder: false);
    }

    private sealed class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class Order
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public Customer? Customer { get; set; }
        public string Status { get; set; } = "";
    }

    private sealed class OrderLine
    {
        public int OrderId { get; set; }
        public int LineId { get; set; }
        public Order? Order { get; set; }
        public string Product { get; set; } = "";
    }

    private sealed class CorrelatedUpdateContext(ListLoggerFactory sqlLoggerFactory) : DbContext
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<OrderLine> OrderLines => Set<OrderLine>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Customer>(builder =>
            {
                builder.ToTable("Customers");
                builder.HasKey(customer => customer.Id);
                builder.Property(customer => customer.Id).ValueGeneratedNever();
            });

            modelBuilder.Entity<Order>(builder =>
            {
                builder.ToTable("Orders");
                builder.HasKey(order => order.Id);
                builder.Property(order => order.Id).ValueGeneratedNever();
                builder.HasOne(order => order.Customer)
                    .WithMany()
                    .HasForeignKey(order => order.CustomerId);
            });

            modelBuilder.Entity<OrderLine>(builder =>
            {
                builder.ToTable("OrderLines");
                builder.HasKey(line => new { line.OrderId, line.LineId });
                builder.Property(line => line.OrderId).HasColumnName("OrderKey").ValueGeneratedNever();
                builder.Property(line => line.LineId).HasColumnName("LineKey").ValueGeneratedNever();
                builder.HasOne(line => line.Order)
                    .WithMany()
                    .HasForeignKey(line => line.OrderId);
            });
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder
                .UseYdb("Host=localhost;Port=2136;Database=/local;UseTls=false")
                .UseLoggerFactory(sqlLoggerFactory)
                .EnableServiceProviderCaching(false);
    }
}
