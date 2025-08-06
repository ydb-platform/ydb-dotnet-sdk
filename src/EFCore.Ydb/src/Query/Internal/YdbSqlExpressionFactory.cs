using System.Diagnostics.CodeAnalysis;
using EntityFrameworkCore.Ydb.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : SqlExpressionFactory(dependencies)
{
    [return: NotNullIfNotNull("sqlExpression")]
    public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping) =>
        base.ApplyTypeMapping(sqlExpression, typeMapping);
    
    public virtual YdbILikeExpression ILike(
        SqlExpression match,
        SqlExpression pattern,
        SqlExpression? escapeChar = null)
        => (YdbILikeExpression)ApplyDefaultTypeMapping(new YdbILikeExpression(match, pattern, escapeChar, null));

}
