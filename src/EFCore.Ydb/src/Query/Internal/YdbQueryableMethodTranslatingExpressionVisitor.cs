using System.Linq;
using EntityFrameworkCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public partial class YdbQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly YdbTypeMappingSource? _typeMappingSource;
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

    private static bool CanGenerateModificationOn(SelectExpression selectExpression)
        => selectExpression.Tables.Count > 0
           && selectExpression.Tables.All(table => table is
               TableExpression or InnerJoinExpression or LeftJoinExpression or CrossJoinExpression);
}
