using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ef.Ydb.Query.Internal.Translators;

public class YdbQueryableAggregateMethodTranslator
    : IAggregateMethodCallTranslator
{
    public YdbQueryableAggregateMethodTranslator(
        YdbSqlExpressionFactory sqlExpressionFactory
    )
    {
    }

    public SqlExpression? Translate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        return null;
    }
}
