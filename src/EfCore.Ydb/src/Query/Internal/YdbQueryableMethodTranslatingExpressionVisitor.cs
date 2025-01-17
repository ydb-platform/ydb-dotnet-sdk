using EfCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly YdbTypeMappingSource _typeMappingSource;
    private readonly YdbSqlExpressionFactory _sqlExpressionFactory;


    public YdbQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext
    ) : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
        _sqlExpressionFactory = (YdbSqlExpressionFactory)relationalDependencies.SqlExpressionFactory;
    }

    private YdbQueryableMethodTranslatingExpressionVisitor(
        YdbQueryableMethodTranslatingExpressionVisitor dependencies
    ) : base(dependencies)
    {
        _queryCompilationContext = dependencies._queryCompilationContext;
        _typeMappingSource = dependencies._typeMappingSource;
        _sqlExpressionFactory = dependencies._sqlExpressionFactory;
    }

    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new YdbQueryableMethodTranslatingExpressionVisitor(this);
}
