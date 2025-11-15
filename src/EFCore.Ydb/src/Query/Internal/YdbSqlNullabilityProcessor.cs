using EntityFrameworkCore.Ydb.Query.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlNullabilityProcessor: SqlNullabilityProcessor
{
	private readonly ISqlExpressionFactory _sqlExpressionFactory;
	public YdbSqlNullabilityProcessor(RelationalParameterBasedSqlProcessorDependencies dependencies, RelationalParameterBasedSqlProcessorParameters parameters) : base(dependencies, parameters)
	{
		_sqlExpressionFactory = dependencies.SqlExpressionFactory;
	}

	protected override SqlExpression VisitCustomSqlExpression(SqlExpression sqlExpression, bool allowOptimizedExpansion,
		out bool nullable) => sqlExpression switch
		{
			YdbILikeExpression ilikeExpression => VisitILikeExpression(ilikeExpression, allowOptimizedExpansion,
				out nullable),
			_ => base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable)
		};

	protected SqlExpression VisitILikeExpression(YdbILikeExpression ilikeExpression, bool allowOptimizedExpansion, out bool nullable)
	{
		var match = Visit(ilikeExpression.Match, out var matchNullable);
        var pattern = Visit(ilikeExpression.Pattern, out var patternNullable);

        nullable = matchNullable || patternNullable;
        SqlExpression result = ilikeExpression.Update(match, pattern);
        
        return result;
	}
}
