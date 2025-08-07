using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlTranslatingExpressionVisitorFactory(
    RelationalSqlTranslatingExpressionVisitorDependencies dependencies
) : IRelationalSqlTranslatingExpressionVisitorFactory
{
    public RelationalSqlTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        => new YdbSqlTranslatingExpressionVisitor(
            dependencies,
            queryCompilationContext,
            queryableMethodTranslatingExpressionVisitor
        );
}
