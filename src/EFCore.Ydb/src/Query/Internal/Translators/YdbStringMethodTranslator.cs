using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbStringMethodTranslator(
    ISqlExpressionFactory sqlExpressionFactory,
    IRelationalTypeMappingSource typeMappingSource) : IMethodCallTranslator
{
    private static readonly RelationalTypeMapping CharMapping =
        (RelationalTypeMapping)new UShortTypeMapping("Uint16", DbType.UInt16)
            .WithComposedConverter(new CastingConverter<char, ushort>());

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
        var character = sqlExpressionFactory.Function(
            "Unicode::Substring",
            [argument, offset, sqlExpressionFactory.Constant(1ul)],
            nullable: true,
            argumentsPropagateNullability: [true, false, false],
            typeof(string));
        var codePoints = sqlExpressionFactory.Function(
            "Unicode::ToCodePointList",
            [character],
            nullable: true,
            argumentsPropagateNullability: [true],
            typeof(uint[]),
            typeMappingSource.FindMapping(typeof(uint[])));
        var codePoint = sqlExpressionFactory.Coalesce(
            sqlExpressionFactory.Function(
                "ListHead",
                [codePoints],
                nullable: true,
                argumentsPropagateNullability: [false],
                typeof(uint)),
            sqlExpressionFactory.Constant(0u));

        // CLR char is a UTF-16 code unit, not a Unicode scalar value.
        var supplementary = sqlExpressionFactory.Subtract(codePoint, sqlExpressionFactory.Constant(0x10000u));
        var surrogate = sqlExpressionFactory.Add(
            isFirst
                ? sqlExpressionFactory.Divide(supplementary, sqlExpressionFactory.Constant(0x400u))
                : sqlExpressionFactory.Modulo(supplementary, sqlExpressionFactory.Constant(0x400u)),
            sqlExpressionFactory.Constant(isFirst ? 0xD800u : 0xDC00u));
        var codeUnit = sqlExpressionFactory.Case(
            [
                new CaseWhenClause(
                    sqlExpressionFactory.GreaterThanOrEqual(codePoint, sqlExpressionFactory.Constant(0x10000u)),
                    surrogate)
            ],
            codePoint);

        return sqlExpressionFactory.Case(
            [
                new CaseWhenClause(
                    sqlExpressionFactory.IsNull(argument),
                    sqlExpressionFactory.Constant(null, typeof(char), CharMapping))
            ],
            sqlExpressionFactory.Convert(codeUnit, typeof(char), CharMapping));
    }
}
