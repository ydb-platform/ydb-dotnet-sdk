using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Ef.Ydb.Query.Internal.Translators;

// TODO: Remove this class. Temporary stub for debug only
public class StubTranslator : IMethodCallTranslator, IMemberTranslator
{
    private readonly YdbSqlExpressionFactory _expressionFactory;

    public StubTranslator(
        YdbSqlExpressionFactory sqlExpressionFactory)
    {
        _expressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        return null;
    }

    public SqlExpression? Translate(SqlExpression? instance, MemberInfo member, Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        return null;
    }
}
