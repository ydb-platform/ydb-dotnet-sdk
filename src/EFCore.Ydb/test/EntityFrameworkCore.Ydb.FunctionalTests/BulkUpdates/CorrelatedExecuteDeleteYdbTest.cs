using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class CorrelatedExecuteDeleteYdbTest
{
    [Fact]
    public async Task Delete_without_correlation_uses_base_sql()
    {
        await using var testStore = CreateStore(nameof(Delete_without_correlation_uses_base_sql));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedDeleteContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Orders.Add(new Order { Id = 1, CustomerId = 1, Status = "Cancelled" });
        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Orders
            .Where(order => order.Status == "Cancelled")
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Orders`
                          WHERE `Status` = 'Cancelled'u
                          """);
        logger.Clear();
        Assert.Empty(await context.Orders.AsNoTracking().ToListAsync());
    }

    [Fact]
    public async Task Delete_with_distinct_without_correlation_uses_base_sql()
    {
        await using var testStore = CreateStore(nameof(Delete_with_distinct_without_correlation_uses_base_sql));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedDeleteContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Orders.Add(new Order { Id = 1, CustomerId = 1, Status = "Cancelled" });
        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Orders
            .Where(order => order.Status == "Cancelled")
            .Distinct()
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Orders`
                          WHERE `Status` = 'Cancelled'u
                          """);
        logger.Clear();
        Assert.Empty(await context.Orders.AsNoTracking().ToListAsync());
    }

    [Fact]
    public async Task Delete_with_navigation_uses_delete_on_instead_of_correlated_subquery()
    {
        await using var testStore =
            CreateStore(nameof(Delete_with_navigation_uses_delete_on_instead_of_correlated_subquery));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedDeleteContext(sqlLoggerFactory);
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
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Orders` ON SELECT `o`.`Id` AS `Id`
                          FROM `Orders` AS `o`
                          INNER JOIN `Customers` AS `c` ON `o`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
        Assert.DoesNotContain("EXISTS", logger.SqlStatements[0]);
        logger.Clear();
        Assert.Equal([20], await context.Orders.AsNoTracking().Select(order => order.Id).ToListAsync());
    }

    [Fact]
    public async Task Delete_with_navigation_projects_every_composite_key_column()
    {
        await using var testStore = CreateStore(nameof(Delete_with_navigation_projects_every_composite_key_column));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new CorrelatedDeleteContext(sqlLoggerFactory);
        await InitializeAsync(testStore, context);

        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        context.Orders.Add(new Order { Id = 10, CustomerId = 1, Status = "Pending" });
        context.OrderLines.AddRange(
            new OrderLine { OrderId = 10, LineId = 1, Product = "Delete" },
            new OrderLine { OrderId = 10, LineId = 2, Product = "Delete" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.OrderLines
            .Where(line => line.Order!.Customer!.Name == "Acme")
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `OrderLines` ON SELECT `o`.`OrderKey` AS `OrderKey`, `o`.`LineKey` AS `LineKey`
                          FROM `OrderLines` AS `o`
                          INNER JOIN `Orders` AS `o0` ON `o`.`OrderKey` = `o0`.`Id`
                          INNER JOIN `Customers` AS `c` ON `o0`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
        logger.Clear();
        Assert.Empty(await context.OrderLines.AsNoTracking().ToListAsync());
    }

    private static async Task InitializeAsync(TestStore testStore, DbContext context)
    {
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
    }

    private static TestStore CreateStore(string testName)
        => YdbTestStoreFactory.Instance.Create($"{nameof(CorrelatedExecuteDeleteYdbTest)}_{testName}");

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

    private sealed class CorrelatedDeleteContext(ListLoggerFactory sqlLoggerFactory) : DbContext
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
