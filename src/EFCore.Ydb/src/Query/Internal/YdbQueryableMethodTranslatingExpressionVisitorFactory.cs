using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbQueryableMethodTranslatingExpressionVisitorFactory(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
    RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies
) : IQueryableMethodTranslatingExpressionVisitorFactory
{
    protected virtual QueryableMethodTranslatingExpressionVisitorDependencies Dependencies { get; } = dependencies;

    protected virtual RelationalQueryableMethodTranslatingExpressionVisitorDependencies
        RelationalDependencies { get; } = relationalDependencies;

    public virtual QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new YdbQueryableMethodTranslatingExpressionVisitor(
            Dependencies,
            RelationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext
        );
}
