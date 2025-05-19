using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbByteArrayMethodTranslator(ISqlExpressionFactory sqlExpressionFactory) : IMethodCallTranslator
{
    private static MethodInfo Contains => typeof(Enumerable)
        .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
        .Where(m => m.Name == nameof(Enumerable.Contains))
        .Single(mi => mi.IsGenericMethod &&
                      mi.GetGenericArguments().Length == 1 &&
                      mi.GetParameters()
                          .Select(e => e.ParameterType)
                          .SequenceEqual(
                              [
                                  typeof(IEnumerable<>).MakeGenericType(mi.GetGenericArguments()[0]),
                                  mi.GetGenericArguments()[0]
                              ]
                          )
        );

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        if (!method.IsGenericMethod
            || !method.GetGenericMethodDefinition().Equals(Contains)
            || arguments[0].Type != typeof(byte[]))
        {
            return null;
        }
        
        var source = arguments[0];

        var value = arguments[1] is SqlConstantExpression constantValue
            ? sqlExpressionFactory.Constant(new[] { (byte)constantValue.Value! }, source.TypeMapping)
            : sqlExpressionFactory.Function(
                "ToBytes",
                [arguments[1]],
                nullable: false,
                argumentsPropagateNullability: ArrayUtil.TrueArrays[1],
                typeof(string));

        return sqlExpressionFactory.IsNotNull(
            sqlExpressionFactory.Function(
                "FIND",
                [source, value],
                nullable: true,
                argumentsPropagateNullability: ArrayUtil.FalseArrays[2],
                typeof(int)
            )
        );

    }
}
