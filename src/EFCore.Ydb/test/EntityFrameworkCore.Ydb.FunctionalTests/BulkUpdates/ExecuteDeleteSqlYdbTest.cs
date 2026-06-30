using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

/// <summary>
/// Verifies YDB-specific DELETE SQL generation (simple WHERE vs DELETE ON for joins).
/// </summary>
public class ExecuteDeleteSqlYdbTest
{
    [Fact]
    public async Task ExecuteDelete_single_table_generates_simple_delete()
    {
        await using var testStore = CreateStore(nameof(ExecuteDelete_single_table_generates_simple_delete));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new SimpleContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Items.Add(new Item { Id = 1, Title = "x" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Items
            .Where(i => i.Id == 1)
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Items`
                          WHERE `Id` = 1
                          """);
    }

    [Fact]
    public async Task ExecuteDelete_with_id_list_generates_where_in()
    {
        await using var testStore = CreateStore(nameof(ExecuteDelete_with_id_list_generates_where_in));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new SimpleContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Items.AddRange(
            new Item { Id = 1, Title = "a" },
            new Item { Id = 2, Title = "b" },
            new Item { Id = 3, Title = "c" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Items
            .Where(i => new[] { 1, 2 }.Contains(i.Id))
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Items`
                          WHERE `Id` IN (1, 2)
                          """);
        Assert.DoesNotContain(" ON ", logger.SqlStatements[0]);
    }

    [Fact]
    public async Task ExecuteDelete_with_subquery_generates_where_in_select()
    {
        await using var testStore = CreateStore(nameof(ExecuteDelete_with_subquery_generates_where_in_select));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new SimpleContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Items.AddRange(
            new Item { Id = 1, Title = "old" },
            new Item { Id = 2, Title = "old" },
            new Item { Id = 3, Title = "keep" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        var items = context.Items;
        await items
            .Where(i => items.Where(x => x.Title == "old").Select(x => x.Id).Contains(i.Id))
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Items`
                          WHERE `Id` IN (
                              SELECT `Id` AS `Id`
                              FROM `Items`
                              WHERE `Title` = 'old'u
                          )
                          """);
        Assert.DoesNotContain(" ON ", logger.SqlStatements[0]);
    }

    [Fact]
    public async Task ExecuteDelete_with_join_generates_delete_on()
    {
        await using var testStore = CreateStore(nameof(ExecuteDelete_with_join_generates_delete_on));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new JoinContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.Customers.Add(new Customer { Id = 1, Name = "Acme" });
        context.Orders.Add(new Order { Id = 10, CustomerId = 1, Status = "Pending" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.Orders
            .Where(o => o.Customer!.Name == "Acme")
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `Orders` ON SELECT `o0`.`Id` AS `Id`
                          FROM `Orders` AS `o0`
                          INNER JOIN `Customers` AS `c` ON `o0`.`CustomerId` = `c`.`Id`
                          WHERE `c`.`Name` = 'Acme'u
                          """);
    }

    private static void AssertSql(TestSqlLoggerFactory logger, string expected)
    {
        if (logger.SqlStatements.Count == 1 && logger.SqlStatements[0] == expected)
        {
            return;
        }

        logger.AssertBaseline([expected], assertOrder: false);
    }

    private static TestStore CreateStore(string testName) =>
        YdbTestStoreFactory.Instance.Create($"{nameof(ExecuteDeleteSqlYdbTest)}_{testName}");

    public class Item
    {
        public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    public class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class Order
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public Customer? Customer { get; set; }
        public string Status { get; set; } = "";
    }

    public class SimpleContext(ListLoggerFactory sqlLoggerFactory) : DbContext
    {
        public DbSet<Item> Items => Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<Item>(b =>
            {
                b.ToTable("Items");
                b.HasKey(i => i.Id);
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .UseLoggerFactory(sqlLoggerFactory)
            .EnableServiceProviderCaching(false);
    }

    public class JoinContext(ListLoggerFactory sqlLoggerFactory) : DbContext
    {
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Order> Orders => Set<Order>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Customer>(b =>
            {
                b.ToTable("Customers");
                b.HasKey(c => c.Id);
            });

            modelBuilder.Entity<Order>(b =>
            {
                b.ToTable("Orders");
                b.HasKey(o => o.Id);
                b.HasOne(o => o.Customer)
                    .WithMany()
                    .HasForeignKey(o => o.CustomerId);
            });
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .UseLoggerFactory(sqlLoggerFactory)
            .EnableServiceProviderCaching(false);
    }
}
