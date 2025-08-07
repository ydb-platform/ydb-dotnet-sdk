using System.Diagnostics.CodeAnalysis;
using EntityFrameworkCore.Ydb.Query.Expressions.Internal;
using EntityFrameworkCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlExpressionFactory : SqlExpressionFactory
{
    private readonly YdbTypeMappingSource _typeMappingSource;
    private readonly RelationalTypeMapping _boolTypeMapping;

    public YdbSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : base(dependencies)
    {
        _typeMappingSource = (YdbTypeMappingSource)dependencies.TypeMappingSource;
        _boolTypeMapping = _typeMappingSource.FindMapping(typeof(bool), dependencies.Model)!; 
    }
    
    [return: NotNullIfNotNull("sqlExpression")]
    public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping)
    {
        if (sqlExpression is not null && sqlExpression.TypeMapping is null)
        {
            sqlExpression = sqlExpression switch
            {
                YdbILikeExpression e => ApplyTypeMappingOnILike(e),
                _ => base.ApplyTypeMapping(sqlExpression, typeMapping),
            };
        }

        return sqlExpression;
    }

    public virtual YdbILikeExpression ILike(
        SqlExpression match,
        SqlExpression pattern,
        SqlExpression? escapeChar = null)
        => (YdbILikeExpression)ApplyDefaultTypeMapping(new YdbILikeExpression(match, pattern, escapeChar, null));

    private SqlExpression ApplyTypeMappingOnILike(YdbILikeExpression ilikeExpression)
    {
        var inferredTypeMapping = (ilikeExpression.EscapeChar is null
                                      ? ExpressionExtensions.InferTypeMapping(
                                          ilikeExpression.Match, ilikeExpression.Pattern)
                                      : ExpressionExtensions.InferTypeMapping(
                                          ilikeExpression.Match, ilikeExpression.Pattern,
                                          ilikeExpression.EscapeChar))
                                  ?? _typeMappingSource.FindMapping(ilikeExpression.Match.Type, Dependencies.Model);

        return new YdbILikeExpression(
            ApplyTypeMapping(ilikeExpression.Match, inferredTypeMapping),
            ApplyTypeMapping(ilikeExpression.Pattern, inferredTypeMapping),
            ApplyTypeMapping(ilikeExpression.EscapeChar, inferredTypeMapping),
            _boolTypeMapping);
    }
}
