using Microsoft.EntityFrameworkCore.Query;

namespace Ef.Ydb.Query.Internal;

public class YdbQueryableMethodTranslatingExpressionVisitorFactory
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    public YdbQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies
    )
    {
        Dependencies = dependencies;
        RelationalDependencies = relationalDependencies;
    }

    protected virtual QueryableMethodTranslatingExpressionVisitorDependencies Dependencies { get; }
    protected virtual RelationalQueryableMethodTranslatingExpressionVisitorDependencies RelationalDependencies { get; }

    public virtual QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new YdbQueryableMethodTranslatingExpressionVisitor(
            Dependencies,
            RelationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext
        );
}
