using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

/// <summary>
/// Verifies YDB-specific DELETE ON SQL generation.
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
                          DELETE FROM `Items` ON SELECT `i`.`Id` AS `Id`
                          FROM `Items` AS `i`
                          WHERE `i`.`Id` = 1
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
                          DELETE FROM `Items` ON SELECT `i`.`Id` AS `Id`
                          FROM `Items` AS `i`
                          WHERE `i`.`Id` IN (1, 2)
                          """);
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
                          DELETE FROM `Items` ON SELECT `i`.`Id` AS `Id`
                          FROM `Items` AS `i`
                          WHERE `i`.`Id` IN (
                              SELECT `i0`.`Id` AS `Id`
                              FROM `Items` AS `i0`
                              WHERE `i0`.`Title` = 'old'u
                          )
                          """);
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
                          DELETE FROM `Orders` ON SELECT `o`.`Id` AS `Id`
                          FROM `Orders` AS `o`
                          WHERE `o`.`Id` IN (
                              SELECT `o0`.`Id` AS `Id`
                              FROM `Orders` AS `o0`
                              INNER JOIN `Customers` AS `c` ON `o0`.`CustomerId` = `c`.`Id`
                              WHERE `c`.`Name` = 'Acme'u
                          )
                          """);
    }

    [Fact]
    public async Task ExecuteDelete_projects_all_renamed_composite_key_columns()
    {
        await using var testStore = CreateStore(nameof(ExecuteDelete_projects_all_renamed_composite_key_columns));
        using var sqlLoggerFactory = YdbTestStoreFactory.Instance.CreateListLoggerFactory(_ => false);
        await using var context = new SimpleContext(sqlLoggerFactory);
        await testStore.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();

        context.CompositeItems.Add(new CompositeItem { PartitionId = 1, ItemId = 2, Title = "delete" });
        await context.SaveChangesAsync();

        var logger = (TestSqlLoggerFactory)sqlLoggerFactory;
        logger.Clear();

        await context.CompositeItems
            .Where(i => i.Title == "delete")
            .ExecuteDeleteAsync();

        AssertSql(logger, """
                          DELETE FROM `CompositeItems` ON SELECT `c`.`PartitionKey` AS `PartitionKey`, `c`.`ItemKey` AS `ItemKey`
                          FROM `CompositeItems` AS `c`
                          WHERE `c`.`Title` = 'delete'u
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

    public class CompositeItem
    {
        public int PartitionId { get; set; }
        public int ItemId { get; set; }
        public string Title { get; set; } = "";
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
        public DbSet<CompositeItem> CompositeItems => Set<CompositeItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>(b =>
            {
                b.ToTable("Items");
                b.HasKey(i => i.Id);
            });

            modelBuilder.Entity<CompositeItem>(b =>
            {
                b.ToTable("CompositeItems");
                b.HasKey(i => new { i.PartitionId, i.ItemId });
                b.Property(i => i.PartitionId).HasColumnName("PartitionKey");
                b.Property(i => i.ItemId).HasColumnName("ItemKey");
            });
        }

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
