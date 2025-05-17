using System;
using System.Collections.Generic;
using System.Reflection;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbDateTimeMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> MethodInfoDatePartMapping = new()
    {
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddYears), [typeof(int)])!, " Years" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddMonths), [typeof(int)])!, " Months" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddDays), [typeof(int)])!, " Days" },

        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddYears), [typeof(int)])!, "Years" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMonths), [typeof(int)])!, "Months" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddDays), [typeof(double)])!, "Days" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddHours), [typeof(double)])!, "Hours" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMinutes), [typeof(double)])!, "Mins" },
        { typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddSeconds), [typeof(double)])!, "Secs" },

        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddYears), [typeof(int)])!, "Years" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMonths), [typeof(int)])!, "Months" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddDays), [typeof(double)])!, "Days" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddHours), [typeof(double)])!, "Hours" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddMinutes), [typeof(double)])!, "Mins" },
        { typeof(DateTimeOffset).GetRuntimeMethod(nameof(DateTimeOffset.AddSeconds), [typeof(double)])!, "Secs" }
    };

    private readonly YdbSqlExpressionFactory _sqlExpressionFactory;

    public YdbDateTimeMethodTranslator(YdbSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }


    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    ) => TranslateDatePart(instance, method, arguments);

    private SqlExpression? TranslateDatePart(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments
    )
    {
        if (
            instance is not null
            && MethodInfoDatePartMapping.TryGetValue(method, out var datePart))
        {
            var shiftDatePartFunction = _sqlExpressionFactory.Function(
                "DateTime::Shift" + datePart,
                [instance, arguments[0]],
                nullable: true,
                ArrayUtil.TrueArrays[2],
                returnType: typeof(DateTime)
            );

            return _sqlExpressionFactory.Function(
                "DateTime::MakeDate",
                arguments: [shiftDatePartFunction],
                nullable: true,
                ArrayUtil.TrueArrays[1],
                returnType: typeof(DateTime)
            );
        }

        return null;
    }
}
