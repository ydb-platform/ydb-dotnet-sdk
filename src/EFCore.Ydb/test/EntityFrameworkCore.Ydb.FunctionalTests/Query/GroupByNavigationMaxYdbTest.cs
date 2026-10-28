using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GroupByNavigationMaxYdbTest
{
    [Fact]
    public async Task Navigation_max_preserves_null_groups_duplicates_and_parameters()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByNavigationMaxYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        int[] count = [0];
        var query = context.Orders.GroupBy(o => o.EmployeeId)
            .Where(g => g.Count() > count[0])
            .Select(g => new { g.Key, Max = g.Max(o => o.Customer!.Region), Count = g.Count() });
        var rows = await query.ToArrayAsync();
        Assert.Equal([((int?)null, (string?)null, 2), (7, "z", 3), (9, "empty", 1)],
            rows.OrderBy(r => r.Key).Select(r => (r.Key, r.Max, r.Count)));

        count[0] = 2;
        rows = await query.ToArrayAsync();
        Assert.Equal([(7, "z", 3)], rows.Select(r => (r.Key, r.Max, r.Count)));

        count[0] = 5;
        Assert.Empty(await query.ToArrayAsync());

        count[0] = 0;
        rows = await query.ToArrayAsync();
        Assert.Equal([((int?)null, (string?)null, 2), (7, "z", 3), (9, "empty", 1)],
            rows.OrderBy(r => r.Key).Select(r => (r.Key, r.Max, r.Count)));

        rows = await query.OrderBy(r => r.Key).Skip(1).Take(1).ToArrayAsync();
        Assert.Equal([(7, "z", 3)], rows.Select(r => (r.Key, r.Max, r.Count)));
    }

    [Fact]
    public async Task Navigation_max_preserves_source_filter_and_enclosing_left_join()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByNavigationMaxYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        int[] count = [0];
        var groups = context.Orders.Where(o => o.Id != 3).GroupBy(o => o.EmployeeId)
            .Where(g => g.Count() > count[0])
            .Select(g => new { g.Key, Max = g.Max(o => o.Customer!.Region), Count = g.Count() });
        var query =
            from employee in context.Employees
            join item in groups on employee.Id equals item.Key into matches
            from item in matches.DefaultIfEmpty()
            orderby employee.Id
            select new LeftRow(employee.Id, item.Max, item.Count);
        var rows = await query.ToArrayAsync();
        Assert.Equal([(7, "dup", (int?)2), (8, null, null), (9, "empty", 1)],
            rows.Select(r => (r.Id, r.Max, r.Count)));

        count[0] = 1;
        rows = await query.ToArrayAsync();
        Assert.Equal([(7, "dup", (int?)2), (8, null, null), (9, null, null)],
            rows.Select(r => (r.Id, r.Max, r.Count)));
    }

    [Fact]
    public async Task Navigation_max_preserves_composite_keys_and_empty_input()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByNavigationMaxYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        int[] firstOrder = [0];
        var query = context.Orders.Where(o => o.Id > firstOrder[0])
            .GroupBy(o => new { o.EmployeeId, o.CustomerId })
            .Select(g => new
            {
                g.Key.EmployeeId, g.Key.CustomerId, Max = g.Max(o => o.Customer!.Region), Count = g.Count()
            });
        var rows = await query.ToArrayAsync();
        Assert.Equal(
            [
                ((int?)null, (int?)null, (string?)null, 1), (null, 3, null, 1), (7, 1, "dup", 2), (7, 2, "z", 1),
                (9, 4, "empty", 1)
            ],
            rows.OrderBy(r => r.EmployeeId).ThenBy(r => r.CustomerId)
                .Select(r => (r.EmployeeId, r.CustomerId, r.Max, r.Count)));

        firstOrder[0] = 99;
        Assert.Empty(await query.ToArrayAsync());
    }

    private sealed record LeftRow(int Id, string? Max, int? Count);

    private static async Task Seed(TestContext context)
    {
        await context.Database.EnsureCreatedAsync();
        context.Customers.AddRange(
            new Customer { Id = 1, Region = "dup" },
            new Customer { Id = 2, Region = "z" },
            new Customer { Id = 3 },
            new Customer { Id = 4, Region = "empty" });
        context.Orders.AddRange(
            new Order { Id = 1, EmployeeId = 7, CustomerId = 1 },
            new Order { Id = 2, EmployeeId = 7, CustomerId = 1 },
            new Order { Id = 3, EmployeeId = 7, CustomerId = 2 },
            new Order { Id = 4, CustomerId = 3 },
            new Order { Id = 5 },
            new Order { Id = 6, EmployeeId = 9, CustomerId = 4 });
        context.Employees.AddRange(new Employee { Id = 7 }, new Employee { Id = 8 }, new Employee { Id = 9 });
        await context.SaveChangesAsync();
        Assert.Equal(6, await context.Orders.CountAsync());
    }

    public class Order
    {
        public int Id { get; set; }
        public int? EmployeeId { get; set; }
        public int? CustomerId { get; set; }
        public Customer? Customer { get; set; }
    }

    public class Customer
    {
        public int Id { get; set; }
        public string? Region { get; set; }
    }

    public class Employee
    {
        public int Id { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<Order> Orders => Set<Order>();
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Employee> Employees => Set<Employee>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model)
        {
            model.Entity<Order>(entity =>
            {
                entity.ToTable("GroupByNavigationMaxOrders");
                entity.Property(o => o.Id).ValueGeneratedNever();
                entity.HasOne(o => o.Customer).WithMany().HasForeignKey(o => o.CustomerId);
            });
            model.Entity<Customer>(entity =>
            {
                entity.ToTable("GroupByNavigationMaxCustomers");
                entity.Property(c => c.Id).ValueGeneratedNever();
            });
            model.Entity<Employee>(entity =>
            {
                entity.ToTable("GroupByNavigationMaxEmployees");
                entity.Property(e => e.Id).ValueGeneratedNever();
            });
        }
    }
}
