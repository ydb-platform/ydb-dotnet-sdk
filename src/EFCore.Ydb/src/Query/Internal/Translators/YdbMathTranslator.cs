using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using ExpressionExtensions = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbMathTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SupportedMethods = new()
    {
        { typeof(Math).GetMethod(nameof(Math.Abs), [typeof(double)])!, "Abs" },
        { typeof(Math).GetMethod(nameof(Math.Abs), [typeof(float)])!, "Abs" },
        { typeof(Math).GetMethod(nameof(Math.Abs), [typeof(int)])!, "Abs" },
        { typeof(Math).GetMethod(nameof(Math.Abs), [typeof(long)])!, "Abs" },
        { typeof(Math).GetMethod(nameof(Math.Abs), [typeof(sbyte)])!, "Abs" },
        { typeof(Math).GetMethod(nameof(Math.Abs), [typeof(short)])!, "Abs" },
        { typeof(Math).GetMethod(nameof(Math.Acos), [typeof(double)])!, "Acos" },
        { typeof(Math).GetMethod(nameof(Math.Acosh), [typeof(double)])!, "Acosh" },
        { typeof(Math).GetMethod(nameof(Math.Asin), [typeof(double)])!, "Asin" },
        { typeof(Math).GetMethod(nameof(Math.Asinh), [typeof(double)])!, "Asinh" },
        { typeof(Math).GetMethod(nameof(Math.Atan), [typeof(double)])!, "Atan" },
        { typeof(Math).GetMethod(nameof(Math.Atan2), [typeof(double), typeof(double)])!, "Atan2" },
        { typeof(Math).GetMethod(nameof(Math.Atanh), [typeof(double)])!, "Atanh" },
        { typeof(Math).GetMethod(nameof(Math.Ceiling), [typeof(double)])!, "Ceil" },
        { typeof(Math).GetMethod(nameof(Math.Cos), [typeof(double)])!, "Cos" },
        { typeof(Math).GetMethod(nameof(Math.Cosh), [typeof(double)])!, "Cosh" },
        { typeof(Math).GetMethod(nameof(Math.Exp), [typeof(double)])!, "Exp" },
        { typeof(Math).GetMethod(nameof(Math.Floor), [typeof(double)])!, "Floor" },
        { typeof(Math).GetMethod(nameof(Math.Log), [typeof(double)])!, "Log" },
        { typeof(Math).GetMethod(nameof(Math.Log2), [typeof(double)])!, "Log2" },
        { typeof(Math).GetMethod(nameof(Math.Log10), [typeof(double)])!, "Log10" },
        { typeof(Math).GetMethod(nameof(Math.Pow), [typeof(double), typeof(double)])!, "Pow" },
        { typeof(Math).GetMethod(nameof(Math.Round), [typeof(double)])!, "Round" },
        { typeof(Math).GetMethod(nameof(Math.Sign), [typeof(double)])!, "Sign" },
        { typeof(Math).GetMethod(nameof(Math.Sign), [typeof(float)])!, "Sign" },
        { typeof(Math).GetMethod(nameof(Math.Sign), [typeof(long)])!, "Sign" },
        { typeof(Math).GetMethod(nameof(Math.Sign), [typeof(sbyte)])!, "Sign" },
        { typeof(Math).GetMethod(nameof(Math.Sign), [typeof(short)])!, "Sign" },
        { typeof(Math).GetMethod(nameof(Math.Sin), [typeof(double)])!, "Sin" },
        { typeof(Math).GetMethod(nameof(Math.Sinh), [typeof(double)])!, "Sinh" },
        { typeof(Math).GetMethod(nameof(Math.Sqrt), [typeof(double)])!, "Sqrt" },
        { typeof(Math).GetMethod(nameof(Math.Tan), [typeof(double)])!, "Tan" },
        { typeof(Math).GetMethod(nameof(Math.Tanh), [typeof(double)])!, "Tanh" },
        { typeof(Math).GetMethod(nameof(Math.Truncate), [typeof(double)])!, "Trunc" },
        { typeof(MathF).GetMethod(nameof(MathF.Acos), [typeof(float)])!, "Acos" },
        { typeof(MathF).GetMethod(nameof(MathF.Acosh), [typeof(float)])!, "Acosh" },
        { typeof(MathF).GetMethod(nameof(MathF.Asin), [typeof(float)])!, "Asin" },
        { typeof(MathF).GetMethod(nameof(MathF.Asinh), [typeof(float)])!, "Asinh" },
        { typeof(MathF).GetMethod(nameof(MathF.Atan), [typeof(float)])!, "Atan" },
        { typeof(MathF).GetMethod(nameof(MathF.Atan2), [typeof(float), typeof(float)])!, "Atan2" },
        { typeof(MathF).GetMethod(nameof(MathF.Atanh), [typeof(float)])!, "Atanh" },
        { typeof(MathF).GetMethod(nameof(MathF.Ceiling), [typeof(float)])!, "Ceil" },
        { typeof(MathF).GetMethod(nameof(MathF.Cos), [typeof(float)])!, "Cos" },
        { typeof(MathF).GetMethod(nameof(MathF.Cosh), [typeof(float)])!, "Cosh" },
        { typeof(MathF).GetMethod(nameof(MathF.Exp), [typeof(float)])!, "Exp" },
        { typeof(MathF).GetMethod(nameof(MathF.Floor), [typeof(float)])!, "Floor" },
        { typeof(MathF).GetMethod(nameof(MathF.Log), [typeof(float)])!, "Log" },
        { typeof(MathF).GetMethod(nameof(MathF.Log10), [typeof(float)])!, "Log10" },
        { typeof(MathF).GetMethod(nameof(MathF.Log2), [typeof(float)])!, "Log2" },
        { typeof(MathF).GetMethod(nameof(MathF.Pow), [typeof(float), typeof(float)])!, "Pow" },
        { typeof(MathF).GetMethod(nameof(MathF.Round), [typeof(float)])!, "Round" },
        { typeof(MathF).GetMethod(nameof(MathF.Sin), [typeof(float)])!, "Sin" },
        { typeof(MathF).GetMethod(nameof(MathF.Sinh), [typeof(float)])!, "Sinh" },
        { typeof(MathF).GetMethod(nameof(MathF.Sqrt), [typeof(float)])!, "Sqrt" },
        { typeof(MathF).GetMethod(nameof(MathF.Tan), [typeof(float)])!, "Tan" },
        { typeof(MathF).GetMethod(nameof(MathF.Tanh), [typeof(float)])!, "Tanh" },
        { typeof(MathF).GetMethod(nameof(MathF.Truncate), [typeof(float)])!, "Trunc" }
    };

    private static readonly List<MethodInfo> RoundWithDecimalMethods =
    [
        typeof(Math).GetMethod(nameof(Math.Round), [typeof(double), typeof(int)])!,
        typeof(MathF).GetMethod(nameof(MathF.Round), [typeof(float), typeof(int)])!
    ];

    private static readonly List<MethodInfo> LogWithBaseMethods =
    [
        typeof(Math).GetMethod(nameof(Math.Log), [typeof(double), typeof(double)])!,
        typeof(MathF).GetMethod(nameof(MathF.Log), [typeof(float), typeof(float)])!
    ];

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public YdbMathTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        if (SupportedMethods.TryGetValue(method, out var sqlFunctionName))
        {
            var typeMapping = ExpressionExtensions.InferTypeMapping(arguments.ToArray());
            var newArguments = arguments
                .Select(a => _sqlExpressionFactory.ApplyTypeMapping(a, typeMapping))
                .ToList();

            return _sqlExpressionFactory.Function(
                "Math::" + sqlFunctionName,
                newArguments,
                nullable: true,
                argumentsPropagateNullability: newArguments.Select(_ => true).ToList(),
                method.ReturnType,
                typeMapping
            );
        }

        if (RoundWithDecimalMethods.Contains(method))
        {
            return _sqlExpressionFactory.Function(
                "Math::Round",
                arguments,
                nullable: true,
                argumentsPropagateNullability: ArrayUtil.TrueArrays[2],
                method.ReturnType,
                arguments[0].TypeMapping);
        }

        return null;
    }
}
