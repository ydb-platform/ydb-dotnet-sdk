using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GroupByNavigationYdbTest
{
    [Fact]
    public async Task Navigation_min_preserves_null_groups_duplicates_and_parameters()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByNavigationYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        var parents = context.Parents;
        int[] min = [1];
        var query = parents
            .GroupBy(p => p.Child.Category.Name, p => new { Value = p.Child.Value ?? 0 })
            .Where(g => g.Min(e => e.Value) > min[0])
            .Select(g => new { g.Key, Count = g.Count() });
        var rows = await query.ToArrayAsync();
        Assert.Equal([("dup", 3)], rows.Select(g => (g.Key, g.Count)));

        min[0] = 2;
        Assert.Empty(await query.ToArrayAsync());

        min[0] = -2;
        rows = await query.ToArrayAsync();
        Assert.Equal(2, rows.Length);
        Assert.Equal(2, rows.Single(g => g.Key == null).Count);
        Assert.Equal(3, rows.Single(g => g.Key == "dup").Count);

        rows = await query.OrderBy(g => g.Key).Skip(1).Take(1).ToArrayAsync();
        Assert.Equal([("dup", 3)], rows.Select(g => (g.Key, g.Count)));
    }

    [Fact]
    public async Task Navigation_min_preserves_source_filter_and_enclosing_left_join()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(GroupByNavigationYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await Seed(context);

        var parents = context.Parents;
        var categories = context.Categories;
        int[] min = [-2];
        var groups = parents.Where(p => p.Id != 5)
            .GroupBy(p => p.Child.Category.Id, p => new { Value = p.Child.Value ?? 0 })
            .Where(g => g.Min(e => e.Value) > min[0])
            .Select(g => new { CategoryId = g.Key, Count = g.Count() });
        var query =
            from category in categories
            join item in groups on category.Id equals item.CategoryId into matches
            from item in matches.DefaultIfEmpty()
            orderby category.Id
            select new { category.Id, Count = (int?)item.Count };
        var rows = await query.ToArrayAsync();
        Assert.Equal([(1, (int?)2), (2, 1), (3, 1), (4, null)], rows.Select(r => (r.Id, r.Count)));

        min[0] = 1;
        rows = await query.ToArrayAsync();
        Assert.Equal([(1, (int?)2), (2, 1), (3, null), (4, null)], rows.Select(r => (r.Id, r.Count)));
    }

    private static async Task Seed(TestContext context)
    {
        await context.Database.EnsureCreatedAsync();
        context.Categories.AddRange(
            new Category { Id = 1, Name = "dup" },
            new Category { Id = 2, Name = "dup" },
            new Category { Id = 3 },
            new Category { Id = 4, Name = "empty" });
        context.Children.AddRange(
            new Child { Id = 10, CategoryId = 1, Value = 2 },
            new Child { Id = 11, CategoryId = 2, Value = 5 },
            new Child { Id = 12, CategoryId = 3 },
            new Child { Id = 13, CategoryId = 3, Value = -1 },
            new Child { Id = 14, CategoryId = 4, Value = 8 });
        context.Parents.AddRange(
            new Parent { Id = 1, ChildId = 10 },
            new Parent { Id = 2, ChildId = 10 },
            new Parent { Id = 3, ChildId = 11 },
            new Parent { Id = 4, ChildId = 12 },
            new Parent { Id = 5, ChildId = 13 });
        await context.SaveChangesAsync();
        Assert.Equal(5, await context.Parents.CountAsync());
    }

    public class Parent
    {
        public int Id { get; set; }
        public int ChildId { get; set; }
        public Child Child { get; set; } = null!;
    }

    public class Child
    {
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public int? Value { get; set; }
        public Category Category { get; set; } = null!;
    }

    public class Category
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<Parent> Parents => Set<Parent>();
        public DbSet<Child> Children => Set<Child>();
        public DbSet<Category> Categories => Set<Category>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model)
        {
            model.Entity<Parent>(entity =>
            {
                entity.ToTable("GroupByNavigationParents");
                entity.Property(p => p.Id).ValueGeneratedNever();
                entity.HasOne(p => p.Child).WithMany().HasForeignKey(p => p.ChildId).IsRequired();
            });
            model.Entity<Child>(entity =>
            {
                entity.ToTable("GroupByNavigationChildren");
                entity.Property(c => c.Id).ValueGeneratedNever();
                entity.HasOne(c => c.Category).WithMany().HasForeignKey(c => c.CategoryId).IsRequired();
            });
            model.Entity<Category>(entity =>
            {
                entity.ToTable("GroupByNavigationCategories");
                entity.Property(c => c.Id).ValueGeneratedNever();
            });
        }
    }
}
