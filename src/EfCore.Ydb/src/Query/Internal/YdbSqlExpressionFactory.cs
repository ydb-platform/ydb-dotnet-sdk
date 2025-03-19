using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Query.Internal;

public class YdbSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : SqlExpressionFactory(dependencies)
{
    [return: NotNullIfNotNull("sqlExpression")]
    public override SqlExpression? ApplyTypeMapping(SqlExpression? sqlExpression, RelationalTypeMapping? typeMapping) =>
        base.ApplyTypeMapping(sqlExpression, typeMapping);
}
