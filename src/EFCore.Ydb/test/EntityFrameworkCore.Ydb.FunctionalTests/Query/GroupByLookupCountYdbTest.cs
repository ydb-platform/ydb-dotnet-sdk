using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GroupByLookupCountYdbTest
{
    [Fact]
    public async Task Lookup_count_preserves_null_groups_duplicates_and_parameters()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByLookupCountYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        int[] minimum = [0];
        var customers = context.Customers;
        var query = context.Orders.GroupBy(o => o.CustomerId)
            .Where(g => g.Count() > minimum[0])
            .Select(g => new
            {
                g.Key, CustomerCount = customers.Count(c => c.Id == g.Key), OrderCount = g.Count()
            });
        var rows = await query.ToArrayAsync();
        Assert.Equal([(null, 0, 2), ("A", 1, 3), ("B", 1, 1), ("C", 1, 1), ("MISSING", 0, 1)],
            rows.OrderBy(r => r.Key).Select(r => (r.Key, r.CustomerCount, r.OrderCount)));

        minimum[0] = 1;
        rows = await query.ToArrayAsync();
        Assert.Equal([(null, 0, 2), ("A", 1, 3)],
            rows.OrderBy(r => r.Key).Select(r => (r.Key, r.CustomerCount, r.OrderCount)));

        minimum[0] = 2;
        rows = await query.ToArrayAsync();
        Assert.Equal([("A", 1, 3)], rows.Select(r => (r.Key, r.CustomerCount, r.OrderCount)));

        minimum[0] = 20;
        Assert.Empty(await query.ToArrayAsync());

        minimum[0] = 0;
        rows = await query.ToArrayAsync();
        Assert.Equal([(null, 0, 2), ("A", 1, 3), ("B", 1, 1), ("C", 1, 1), ("MISSING", 0, 1)],
            rows.OrderBy(r => r.Key).Select(r => (r.Key, r.CustomerCount, r.OrderCount)));

        rows = await query.OrderBy(r => r.Key).Skip(1).Take(1).ToArrayAsync();
        Assert.Equal([("A", 1, 3)], rows.Select(r => (r.Key, r.CustomerCount, r.OrderCount)));
    }

    [Fact]
    public async Task Lookup_count_preserves_source_filter_and_enclosing_left_join()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByLookupCountYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        int[] minimum = [0];
        var customers = context.Customers;
        var groups = context.Orders.Where(o => o.Id != 3).GroupBy(o => o.CustomerId)
            .Where(g => g.Count() > minimum[0])
            .Select(g => new
            {
                g.Key, CustomerCount = customers.Count(c => c.Id == g.Key), OrderCount = g.Count()
            });
        var rows = await groups.ToArrayAsync();
        Assert.Equal([(null, 0, 2), ("A", 1, 3), ("C", 1, 1), ("MISSING", 0, 1)],
            rows.OrderBy(r => r.Key).Select(r => (r.Key, r.CustomerCount, r.OrderCount)));

        var query =
            from customer in customers
            join item in groups on customer.Id equals item.Key into matches
            from item in matches.DefaultIfEmpty()
            orderby customer.Id
            select new LeftRow(customer.Id, item.CustomerCount, item.OrderCount);
        var left = await query.ToArrayAsync();
        Assert.Equal([("A", (int?)1, (int?)3), ("B", null, null), ("C", 1, 1), ("D", null, null)],
            left.Select(r => (r.Id, r.CustomerCount, r.OrderCount)));

        minimum[0] = 1;
        left = await query.ToArrayAsync();
        Assert.Equal([("A", (int?)1, (int?)3), ("B", null, null), ("C", null, null), ("D", null, null)],
            left.Select(r => (r.Id, r.CustomerCount, r.OrderCount)));
    }

    [Fact]
    public async Task Lookup_count_preserves_composite_groups_and_empty_input()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByLookupCountYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        int[] firstOrder = [0];
        var customers = context.Customers;
        var query = context.Orders.Where(o => o.Id > firstOrder[0])
            .GroupBy(o => new { o.CustomerId, o.EmployeeId })
            .Select(g => new
            {
                g.Key.CustomerId, g.Key.EmployeeId,
                CustomerCount = customers.Count(c => c.Id == g.Key.CustomerId), OrderCount = g.Count()
            });
        var rows = await query.ToArrayAsync();
        Assert.Equal(
            [
                ((string?)null, (int?)null, 0, 2), ("A", 7, 1, 2), ("A", 8, 1, 1), ("B", 7, 1, 1),
                ("C", 9, 1, 1), ("MISSING", 9, 0, 1)
            ],
            rows.OrderBy(r => r.CustomerId).ThenBy(r => r.EmployeeId)
                .Select(r => (r.CustomerId, r.EmployeeId, r.CustomerCount, r.OrderCount)));

        firstOrder[0] = 99;
        Assert.Empty(await query.ToArrayAsync());
    }

    private sealed record LeftRow(string Id, int? CustomerCount, int? OrderCount);

    private static async Task Seed(TestContext context)
    {
        await context.Database.EnsureCreatedAsync();
        context.Customers.AddRange(
            new Customer { Id = "A", Name = "alpha" },
            new Customer { Id = "B", Name = "beta" },
            new Customer { Id = "C" },
            new Customer { Id = "D", Name = "empty" });
        context.Orders.AddRange(
            new Order { Id = 1, CustomerId = "A", EmployeeId = 7 },
            new Order { Id = 2, CustomerId = "A", EmployeeId = 7 },
            new Order { Id = 3, CustomerId = "B", EmployeeId = 7 },
            new Order { Id = 4 },
            new Order { Id = 5 },
            new Order { Id = 6, CustomerId = "MISSING", EmployeeId = 9 },
            new Order { Id = 7, CustomerId = "C", EmployeeId = 9 },
            new Order { Id = 8, CustomerId = "A", EmployeeId = 8 });
        await context.SaveChangesAsync();
        Assert.Equal(8, await context.Orders.CountAsync());
    }

    public class Order
    {
        public int Id { get; set; }
        public string? CustomerId { get; set; }
        public int? EmployeeId { get; set; }
    }

    public class Customer
    {
        public string Id { get; set; } = null!;
        public string? Name { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Customer> Customers => Set<Customer>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model)
        {
            model.Entity<Order>(entity =>
            {
                entity.ToTable("GroupByLookupCountOrders");
                entity.Property(o => o.Id).ValueGeneratedNever();
            });
            model.Entity<Customer>(entity =>
            {
                entity.ToTable("GroupByLookupCountCustomers");
                entity.Property(c => c.Id).ValueGeneratedNever();
            });
        }
    }
}
