using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ef.Ydb.Query.Internal;

public class YdbSqlExpressionFactory
    : SqlExpressionFactory
{
    public YdbSqlExpressionFactory(
        SqlExpressionFactoryDependencies dependencies
    ) : base(dependencies)
    {
    }

    [return: NotNullIfNotNull("sqlExpression")]
    public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping)
    {
        return base.ApplyTypeMapping(sqlExpression, typeMapping);
    }
}
