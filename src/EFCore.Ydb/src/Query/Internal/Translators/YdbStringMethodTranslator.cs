using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory) : IMethodCallTranslator
{
    private static readonly RelationalTypeMapping FirstCharMapping =
        (RelationalTypeMapping)YdbTextTypeMapping.Default.WithComposedConverter(new CharToStringConverter());

    private static readonly RelationalTypeMapping LastCharMapping =
        (RelationalTypeMapping)YdbTextTypeMapping.Default.WithComposedConverter(
            new ValueConverter<char, string>(
                value => value.ToString(),
                value => value.Length == 0 ? '\0' : value[value.Length - 1]));

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(Enumerable)
            || method.ReturnType != typeof(char)
            || arguments.Count != 1
            || arguments[0].Type != typeof(string)
            || method.Name is not (nameof(Enumerable.FirstOrDefault) or nameof(Enumerable.LastOrDefault)))
        {
            return null;
        }

        var argument = arguments[0];
        var isFirst = method.Name == nameof(Enumerable.FirstOrDefault);
        var offset = isFirst
            ? sqlExpressionFactory.Constant(0ul)
            : sqlExpressionFactory.Case(
                [
                    new CaseWhenClause(
                        sqlExpressionFactory.Equal(argument, sqlExpressionFactory.Constant(string.Empty)),
                        sqlExpressionFactory.Constant(0ul))
                ],
                sqlExpressionFactory.Subtract(
                    sqlExpressionFactory.Function(
                        "Unicode::GetLength",
                        [argument],
                        nullable: true,
                        argumentsPropagateNullability: [true],
                        typeof(ulong)),
                    sqlExpressionFactory.Constant(1ul)));
        return sqlExpressionFactory.Function(
            "Unicode::Substring",
            [argument, offset, sqlExpressionFactory.Constant(1ul)],
            nullable: true,
            argumentsPropagateNullability: [true, false, false],
            typeof(char),
            isFirst ? FirstCharMapping : LastCharMapping);
    }
}
