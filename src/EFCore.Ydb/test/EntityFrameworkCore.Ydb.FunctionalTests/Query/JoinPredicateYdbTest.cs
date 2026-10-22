using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class JoinPredicateYdbTest
{
    [Fact]
    public async Task Inner_join_filter_preserves_outer_rows_and_duplicates()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(JoinPredicateYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 1, Key = 10, Value = 10 },
            new TestEntity { Id = 2, Key = 10, Value = 20 },
            new TestEntity { Id = 3, Key = 20, Value = 10 },
            new TestEntity { Id = 4, Key = 30, Value = 0 },
            new TestEntity { Id = 5 },
            new TestEntity { Id = 6, Key = 10, Value = 20 });
        await context.SaveChangesAsync();

        var ids = await context.Entities.OrderBy(e => e.Id).Select(e => e.Id).ToArrayAsync();
        Assert.Equal([1, 2, 3, 4, 5, 6], ids);

        var entities = context.Entities;
        var inner =
            from left in entities
            from right in entities.Where(right => right.Key == left.Key && right.Value > left.Value)
            select new { OuterId = left.Id, InnerId = right.Id };
        var result = await (
            from parent in entities
            join match in inner on parent.Id equals match.OuterId into matches
            from match in matches.DefaultIfEmpty()
            where parent.Id <= 5
            orderby parent.Id, match.InnerId
            select new { parent.Id, MatchId = (int?)match.InnerId }).ToArrayAsync();

        Assert.Equal([(1, (int?)2), (1, 6), (2, null), (3, null), (4, null), (5, null)],
            result.Select(e => (e.Id, e.MatchId)));
    }

    [Fact]
    public async Task Inner_join_disjunction_preserves_null_matches()
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(JoinPredicateYdbTest));
        await using var context = new TestContext();
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Entities.AddRange(
            new TestEntity { Id = 1, Key = 10, Value = 10 },
            new TestEntity { Id = 2, Key = 10, Value = 20 },
            new TestEntity { Id = 3, Key = 10 });
        await context.SaveChangesAsync();

        Assert.Equal(3, await context.Entities.CountAsync());
        Assert.Equal(10, await context.Entities.Select(e => e.Key).Distinct().SingleAsync());

        var entities = context.Entities;
        var result = await (
            from left in entities
            from right in entities.Where(right => right.Key == left.Key
                                                  && (right.Value > left.Value || right.Value == null))
            orderby left.Id, right.Id
            select new { Left = left.Id, Right = right.Id }).ToArrayAsync();

        Assert.Equal([(1, 2), (1, 3), (2, 3), (3, 3)], result.Select(e => (e.Left, e.Right)));
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public int? Key { get; set; }
        public int? Value { get; set; }
    }

    public class TestContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnConfiguring(DbContextOptionsBuilder options) =>
            options.UseYdb("Host=localhost;Port=2136");

        protected override void OnModelCreating(ModelBuilder model) => model.Entity<TestEntity>(entity =>
        {
            entity.ToTable("JoinPredicateEntities");
            entity.Property(e => e.Id).ValueGeneratedNever();
        });
    }
}
