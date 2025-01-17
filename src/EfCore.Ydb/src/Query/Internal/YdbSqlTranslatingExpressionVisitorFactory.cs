using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    private readonly RelationalSqlTranslatingExpressionVisitorDependencies Dependencies;

    public YdbSqlTranslatingExpressionVisitorFactory(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies
    )
    {
        Dependencies = dependencies;
    }

    public RelationalSqlTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        => new YdbSqlTranslatingExpressionVisitor(
            Dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor
        );
}
