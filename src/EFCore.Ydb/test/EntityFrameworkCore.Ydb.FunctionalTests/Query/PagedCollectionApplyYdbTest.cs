using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class PagedCollectionApplyYdbTest
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task Ordered_child_page_preserves_filters_duplicates_nulls_and_empty_parents(int skip)
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(PagedCollectionApplyYdbTest));
        var options = store.AddProviderOptions(new DbContextOptionsBuilder()).Options;
        await using var context = new TestContext(options);
        await store.CleanAsync(context);
        await context.Database.EnsureCreatedAsync();
        context.Parents.AddRange(
            new Parent { Id = 1, Name = "first" },
            new Parent { Id = 2, Name = "second" },
            new Parent { Id = 3, Name = "empty" },
            new Parent { Id = 4, Name = "filtered" },
            new Parent { Id = 5, Name = "short" });
        context.Children.AddRange(
            new Child { Id = 10, ParentId = 1, LookupId = 2, Rank = 0 },
            new Child { Id = 11, ParentId = 1, LookupId = 2, Rank = 1, Active = true },
            new Child { Id = 12, ParentId = 1, LookupId = 2, Rank = 2, Active = true, Value = "dup" },
            new Child { Id = 13, ParentId = 1, LookupId = 2, Rank = 2, Active = true, Value = "dup" },
            new Child { Id = 14, ParentId = 1, LookupId = 2, Rank = 3, Active = true, Value = "tail" },
            new Child { Id = 21, ParentId = 2, LookupId = 1, Rank = 1, Active = true, Value = "a" },
            new Child { Id = 22, ParentId = 2, LookupId = 1, Rank = 2, Active = true },
            new Child { Id = 23, ParentId = 2, LookupId = 1, Rank = 3, Active = true, Value = "b" },
            new Child { Id = 40, ParentId = 4, LookupId = 1, Rank = 1 },
            new Child { Id = 50, ParentId = 5, LookupId = 1, Rank = 1, Active = true, Value = "only" });
        await context.SaveChangesAsync();

        var rows = await Query(context, skip).ToArrayAsync();

        Assert.Equal([(1, "first"), (2, "second"), (3, "empty"), (4, "filtered"), (5, "short")],
            rows.Select(row => (row.Id, row.Name)));
        ChildRow[] expected = skip == 0
            ?
            [
                new ChildRow(11, 1, 2, null, "second"), new ChildRow(12, 1, 2, "dup", "second"),
                new ChildRow(21, 2, 1, "a", "first"), new ChildRow(22, 2, 1, null, "first"),
                new ChildRow(50, 5, 1, "only", "first")
            ]
            :
            [
                new ChildRow(12, 1, 2, "dup", "second"), new ChildRow(13, 1, 2, "dup", "second"),
                new ChildRow(22, 2, 1, null, "first"), new ChildRow(23, 2, 1, "b", "first")
            ];
        Assert.Equal(
            expected.Select(child => (child.Id, child.ParentId, child.LookupId, child.Value, child.LookupName)),
            rows.SelectMany(row => row.Children)
                .Select(child => (child.Id, child.ParentId, child.LookupId, child.Value, child.LookupName)));
        Assert.All(rows, row => Assert.All(row.Children, child => Assert.Equal(row.Id, child.ParentId)));
    }

    [Theory]
    [InlineData(CorrelationLocation.Predicate)]
    [InlineData(CorrelationLocation.Ordering)]
    [InlineData(CorrelationLocation.Projection)]
    public async Task Additional_outer_correlation_is_not_lowered(CorrelationLocation location)
    {
        await using var store = YdbTestStoreFactory.Instance.Create(nameof(PagedCollectionApplyYdbTest));
        var options = store.AddProviderOptions(new DbContextOptionsBuilder())
            .EnableServiceProviderCaching(false)
            .ReplaceService<IRelationalParameterBasedSqlProcessorFactory,
                GroupByAggregateRewriterYdbTest.CapturingProcessorFactory>().Options;
        using var context = new TestContext(options);
        _ = Query(context, 1).ToQueryString();
        var processor = Assert.IsType<GroupByAggregateRewriterYdbTest.CapturingProcessorFactory>(
            context.GetService<IRelationalParameterBasedSqlProcessorFactory>()).Processor;
        var select = processor.Input;
        var parent = Assert.IsType<SelectExpression>(select.Tables[0]);
        var apply = Assert.IsType<OuterApplyExpression>(select.Tables[1]);
        var inner = Assert.IsType<SelectExpression>(apply.Table);
        var page = Assert.IsType<SelectExpression>(inner.Tables[0]);
        var outerKey = Assert.IsType<ColumnExpression>(select.Projection[0].Expression);
        var sql = context.GetService<ISqlExpressionFactory>();
        var predicate = page.Predicate;
        var orderings = page.Orderings;
        var projections = page.Projection;
        switch (location)
        {
            case CorrelationLocation.Predicate:
                predicate = sql.AndAlso(predicate!, sql.GreaterThan(outerKey, sql.Constant(0)));
                break;
            case CorrelationLocation.Ordering:
                orderings = [new OrderingExpression(outerKey, true)];
                break;
            case CorrelationLocation.Projection:
                projections = projections.Append(new ProjectionExpression(outerKey, "outer_value")).ToArray();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(location));
        }

        page = page.Update(page.Tables, predicate, page.GroupBy, page.Having, projections,
            orderings, sql.Constant(1), sql.Constant(2));
        inner = inner.Update([page, inner.Tables[1]], inner.Predicate, inner.GroupBy, inner.Having,
            inner.Projection, inner.Orderings, inner.Offset, inner.Limit);
        parent = parent.Update(parent.Tables, parent.Predicate, parent.GroupBy, parent.Having, parent.Projection,
            parent.Orderings, null, sql.Constant(5));
        select = select.Update([parent, new OuterApplyExpression(inner)], select.Predicate,
            select.GroupBy, select.Having, select.Projection, select.Orderings, select.Offset, select.Limit);

        var result = processor.Run(select);

        Assert.IsType<OuterApplyExpression>(result.Tables[1]);
    }

    private static IQueryable<ParentRow> Query(TestContext context, int skip)
        => context.Parents.OrderBy(parent => parent.Id).Take(5)
            .Select(parent => new ParentRow(parent.Id, parent.Name,
                parent.Children.Where(child => child.Active).OrderBy(child => child.Rank).ThenBy(child => child.Id)
                    .Skip(skip).Take(2)
                    .Select(child => new ChildRow(child.Id, child.ParentId, child.LookupId,
                        child.Value, child.Lookup.Name)).ToList()));

    public record ParentRow(int Id, string Name, List<ChildRow> Children);

    public record ChildRow(int Id, int ParentId, int LookupId, string? Value, string LookupName);

    public enum CorrelationLocation
    {
        Predicate,
        Ordering,
        Projection
    }

    public class Parent
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = [];
    }

    public class Child
    {
        public int Id { get; set; }
        public int ParentId { get; set; }
        public int LookupId { get; set; }
        public int Rank { get; set; }
        public bool Active { get; set; }
        public string? Value { get; set; }
        public Parent Lookup { get; set; } = null!;
    }

    public class TestContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Parent> Parents => Set<Parent>();
        public DbSet<Child> Children => Set<Child>();

        protected override void OnModelCreating(ModelBuilder model)
        {
            model.Entity<Parent>().ToTable("PagedParents").Property(parent => parent.Id).ValueGeneratedNever();
            model.Entity<Child>(entity =>
            {
                entity.ToTable("PagedChildren");
                entity.Property(child => child.Id).ValueGeneratedNever();
                entity.HasOne<Parent>().WithMany(parent => parent.Children).HasForeignKey(child => child.ParentId);
                entity.HasOne(child => child.Lookup).WithMany().HasForeignKey(child => child.LookupId);
            });
        }
    }
}
