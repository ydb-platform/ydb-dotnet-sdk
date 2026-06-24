using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using ExpressionExtensions = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

/// <summary>
/// Translates string functions to YQL. Concatenation uses the YDB <c>||</c> operator,
/// matching the Hibernate dialect's <c>concat</c> function mapping.
/// </summary>
public class YdbStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory) : IMethodCallTranslator
{
    private static readonly MethodInfo ConcatMethod = typeof(string).GetRuntimeMethod(
        nameof(string.Concat),
        [typeof(string[])]
    )!;

    private static readonly MethodInfo[] ConcatMethods =
    [
        typeof(string).GetRuntimeMethod(nameof(string.Concat), [typeof(string), typeof(string)])!,
        typeof(string).GetRuntimeMethod(nameof(string.Concat), [typeof(string), typeof(string), typeof(string)])!,
        typeof(string).GetRuntimeMethod(
            nameof(string.Concat),
            [typeof(string), typeof(string), typeof(string), typeof(string)]
        )!,
        ConcatMethod
    ];

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        if (!ConcatMethods.Contains(method))
        {
            return null;
        }

        var stringTypeMapping = ExpressionExtensions.InferTypeMapping(arguments.ToArray());

        SqlExpression? result = null;
        foreach (var argument in arguments)
        {
            var typedArgument = stringTypeMapping is null
                ? argument
                : sqlExpressionFactory.ApplyTypeMapping(argument, stringTypeMapping);
            result = result is null
                ? typedArgument
                : sqlExpressionFactory.Add(result, typedArgument);
        }

        return result;
    }
}
