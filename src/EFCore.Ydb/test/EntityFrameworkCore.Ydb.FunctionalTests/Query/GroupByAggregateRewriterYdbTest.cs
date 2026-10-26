using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GroupByAggregateRewriterYdbTest
{
    [Fact]
    public void Matching_navigation_min_is_inlined()
    {
        using var context = new CapturingContext();
        var select = CreateSelect(context, UnsafeInput.None);

        var result = Process(context, select);

        var aggregate = Assert.IsType<SqlFunctionExpression>(
            Assert.IsType<SqlBinaryExpression>(result.Having).Left);
        Assert.Equal("MIN", aggregate.Name);
    }

    [Theory]
    [InlineData(UnsafeInput.GroupedAggregate)]
    [InlineData(UnsafeInput.PrefixJoinKind)]
    [InlineData(UnsafeInput.PrefixJoinKey)]
    [InlineData(UnsafeInput.LookupJoinKind)]
    [InlineData(UnsafeInput.LookupJoinKey)]
    [InlineData(UnsafeInput.CrossLookup)]
    [InlineData(UnsafeInput.MissingSourceFilter)]
    [InlineData(UnsafeInput.MissingGroupCorrelation)]
    [InlineData(UnsafeInput.AdditionalGroupFilter)]
    public void Different_aggregate_input_is_not_inlined(UnsafeInput input)
    {
        using var context = new CapturingContext();
        var select = CreateSelect(context, input);

        var result = Process(context, select);

        var scalar = Assert.IsType<ScalarSubqueryExpression>(
            Assert.IsType<SqlBinaryExpression>(result.Having).Left);
        Assert.Equal("MIN", Assert.IsType<SqlFunctionExpression>(scalar.Subquery.Projection[0].Expression).Name);
    }

    private static SelectExpression CreateSelect(CapturingContext context, UnsafeInput input)
    {
        _ = context.Parents
            .GroupBy(p => p.Child.Category.Name, p => new { Value = p.Child.Value ?? 0 })
            .Where(g => g.Min(e => e.Value) > 1)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToQueryString();
        var factory = Assert.IsType<CapturingProcessorFactory>(
            context.GetService<IRelationalParameterBasedSqlProcessorFactory>());
        var select = factory.Processor.Input;
        var having = Assert.IsType<SqlBinaryExpression>(select.Having);
        var inner = Assert.IsType<ScalarSubqueryExpression>(having.Left).Subquery;
        var sql = context.GetService<ISqlExpressionFactory>();
        var parent = Assert.IsType<TableExpression>(select.Tables[0]);
        var innerTables = inner.Tables.ToArray();
        var innerParent = Assert.IsType<TableExpression>(innerTables[0]);
        var prefixJoin = (InnerJoinExpression)innerTables[1];
        var lookupJoin = (InnerJoinExpression)innerTables[3];
        var innerChild = Assert.IsType<TableExpression>(prefixJoin.Table);
        var lookup = Assert.IsType<TableExpression>(lookupJoin.Table);
        switch (input)
        {
            case UnsafeInput.PrefixJoinKind:
                innerTables[1] = new LeftJoinExpression(innerChild, prefixJoin.JoinPredicate);
                break;
            case UnsafeInput.PrefixJoinKey:
                innerTables[1] = prefixJoin.Update(innerChild,
                    sql.Equal(Column(innerParent, "Id"), Column(innerChild, "Id")));
                break;
            case UnsafeInput.LookupJoinKind:
                innerTables[3] = new LeftJoinExpression(lookup, lookupJoin.JoinPredicate);
                break;
            case UnsafeInput.LookupJoinKey:
                innerTables[3] = lookupJoin.Update(lookup,
                    sql.Equal(Column(innerParent, "ChildId"), Column(lookup, "CategoryId")));
                break;
            case UnsafeInput.CrossLookup:
                innerTables[3] = new CrossJoinExpression(lookup);
                break;
        }

        var innerPredicate = input == UnsafeInput.MissingGroupCorrelation
            ? null
            : inner.Predicate;
        if (input == UnsafeInput.AdditionalGroupFilter)
        {
            innerPredicate = sql.AndAlso(innerPredicate!,
                sql.GreaterThan(Column(innerParent, "Id"), sql.Constant(2)));
        }

        inner = inner.Update(innerTables, innerPredicate,
            input == UnsafeInput.GroupedAggregate ? [Column(innerParent, "Id")] : inner.GroupBy,
            inner.Having, inner.Projection, inner.Orderings, inner.Offset, inner.Limit);
        var predicate = input == UnsafeInput.MissingSourceFilter
            ? sql.GreaterThan(Column(parent, "Id"), sql.Constant(0))
            : select.Predicate;
        return select.Update(select.Tables, predicate, select.GroupBy,
            having.Update(new ScalarSubqueryExpression(inner), having.Right), select.Projection,
            select.Orderings, select.Offset, select.Limit);
    }

    private static ColumnExpression Column(TableExpression table, string name)
    {
        var column = Assert.IsAssignableFrom<ITable>(table.Table).FindColumn(name)!;
#if EFCORE9
        return new ColumnExpression(name, table.Alias, column.ProviderClrType, column.StoreTypeMapping,
            column.IsNullable);
#else
        return new ColumnExpression(name, table.Alias, column, column.ProviderClrType, column.StoreTypeMapping,
            column.IsNullable);
#endif
    }

    private static SelectExpression Process(DbContext context, SelectExpression select)
    {
        var factory = Assert.IsType<CapturingProcessorFactory>(
            context.GetService<IRelationalParameterBasedSqlProcessorFactory>());
        return factory.Processor.Run(select);
    }

    private sealed class CapturingContext : GroupByNavigationYdbTest.TestContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            base.OnConfiguring(options);
            options.EnableServiceProviderCaching(false)
                .ReplaceService<IRelationalParameterBasedSqlProcessorFactory, CapturingProcessorFactory>();
        }
    }

    public sealed class CapturingProcessorFactory(RelationalParameterBasedSqlProcessorDependencies dependencies)
        : IRelationalParameterBasedSqlProcessorFactory, IDisposable
    {
        private readonly GroupByNavigationYdbTest.TestContext _context = new();

        public CapturingProcessor Processor { get; private set; } = null!;

        public RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
            => Processor = new CapturingProcessor(dependencies, parameters,
                _context.GetService<IRelationalParameterBasedSqlProcessorFactory>().Create(parameters));

        public void Dispose() => _context.Dispose();
    }

    public sealed class CapturingProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters,
        RelationalParameterBasedSqlProcessor processor)
        : RelationalParameterBasedSqlProcessor(dependencies, parameters)
    {
        public SelectExpression Input { get; private set; } = null!;

#if EFCORE9
        public override Expression Optimize(Expression queryExpression,
            IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        {
            Input = Assert.IsType<SelectExpression>(queryExpression);
            return processor.Optimize(queryExpression, parametersValues, out canCache);
        }

        public SelectExpression Run(SelectExpression select)
            => (SelectExpression)Optimize(select, new Dictionary<string, object?>(), out _);
#else
        public override Expression Process(Expression queryExpression, ParametersCacheDecorator parametersDecorator)
        {
            Input = Assert.IsType<SelectExpression>(queryExpression);
            return processor.Process(queryExpression, parametersDecorator);
        }

        public SelectExpression Run(SelectExpression select)
            => (SelectExpression)Process(select, new ParametersCacheDecorator(new Dictionary<string, object?>()));
#endif
    }

    public enum UnsafeInput
    {
        None,
        GroupedAggregate,
        PrefixJoinKind,
        PrefixJoinKey,
        LookupJoinKind,
        LookupJoinKey,
        CrossLookup,
        MissingSourceFilter,
        MissingGroupCorrelation,
        AdditionalGroupFilter
    }
}
