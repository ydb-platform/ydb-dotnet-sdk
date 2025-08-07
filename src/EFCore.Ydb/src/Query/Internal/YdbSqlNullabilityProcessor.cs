using EntityFrameworkCore.Ydb.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlNullabilityProcessor : SqlNullabilityProcessor
{
    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public YdbSqlNullabilityProcessor(RelationalParameterBasedSqlProcessorDependencies dependencies, 
        RelationalParameterBasedSqlProcessorParameters parameters) : base(dependencies, parameters)
    {
        _sqlExpressionFactory = dependencies.SqlExpressionFactory;
    }
    
        protected virtual SqlExpression VisitILike(YdbILikeExpression iLikeExpression, bool allowOptimizedExpansion, out bool nullable)
    {
        // Note: this is largely duplicated from relational SqlNullabilityProcessor.VisitLike.
        // We unfortunately can't reuse that since it may return arbitrary expression tree structures with LikeExpression embedded, but
        // we need ILikeExpression (see #3034).
        var match = Visit(iLikeExpression.Match, out var matchNullable);
        var pattern = Visit(iLikeExpression.Pattern, out var patternNullable);
        var escapeChar = Visit(iLikeExpression.EscapeChar, out var escapeCharNullable);

        SqlExpression result = iLikeExpression.Update(match, pattern, escapeChar);

        if (UseRelationalNulls)
        {
            nullable = matchNullable || patternNullable || escapeCharNullable;

            return result;
        }

        nullable = false;

        // The null semantics behavior we implement for LIKE is that it only returns true when both sides are non-null and match; any other
        // input returns false:
        // foo LIKE f% -> true
        // foo LIKE null -> false
        // null LIKE f% -> false
        // null LIKE null -> false

        if (IsNull(match) || IsNull(pattern) || IsNull(escapeChar))
        {
            return _sqlExpressionFactory.Constant(false, iLikeExpression.TypeMapping);
        }

        // A constant match-all pattern (%) returns true for all cases, except where the match string is null:
        // nullable_foo LIKE % -> foo IS NOT NULL
        // non_nullable_foo LIKE % -> true
        if (pattern is SqlConstantExpression { Value: "%" })
        {
            return matchNullable
                ? _sqlExpressionFactory.IsNotNull(match)
                : _sqlExpressionFactory.Constant(true, iLikeExpression.TypeMapping);
        }

        if (!allowOptimizedExpansion)
        {
            if (matchNullable)
            {
                result = _sqlExpressionFactory.AndAlso(result, GenerateNotNullCheck(match));
            }

            if (patternNullable)
            {
                result = _sqlExpressionFactory.AndAlso(result, GenerateNotNullCheck(pattern));
            }

            if (escapeChar is not null && escapeCharNullable)
            {
                result = _sqlExpressionFactory.AndAlso(result, GenerateNotNullCheck(escapeChar));
            }

            // TODO: This revisits the operand; ideally we'd call ProcessNullNotNull directly but that's private
            SqlExpression GenerateNotNullCheck(SqlExpression operand)
                => _sqlExpressionFactory.Not(
                    Visit(_sqlExpressionFactory.IsNull(operand), allowOptimizedExpansion, out _));
        }

        return result;
    }
        
    private bool IsNull(SqlExpression? expression)
        => expression is SqlConstantExpression { Value: null }
           || expression is SqlParameterExpression { Name: string parameterName } && ParametersFacade.IsParameterNull(parameterName);
}
