using System;
using System.Collections.Generic;
using System.Reflection;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbDateTimeMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> MethodInfoDatePartMapping = new()
    {
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddYears), [typeof(int)])!, "Years" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddMonths), [typeof(int)])!, "Months" },
        { typeof(DateOnly).GetRuntimeMethod(nameof(DateOnly.AddDays), [typeof(int)])!, "Days" },

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
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public YdbDateTimeMethodTranslator(
        IRelationalTypeMappingSource typeMappingSource,
        YdbSqlExpressionFactory sqlExpressionFactory)
    {
        _typeMappingSource = typeMappingSource;
        _sqlExpressionFactory = sqlExpressionFactory;
    }


    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        if (instance is not null
            && method.DeclaringType == typeof(TimeOnly)
            && method.Name is nameof(TimeOnly.AddHours) or nameof(TimeOnly.AddMinutes)
            && arguments.Count == 1)
        {
            if (instance.TypeMapping is { Converter: not TimeOnlyToTicksConverter })
            {
                return null;
            }

            var timeMapping = (RelationalTypeMapping)_typeMappingSource.FindMapping(typeof(long))!
                .WithComposedConverter(new TimeOnlyToTicksConverter());
            var ticksPerUnit = method.Name == nameof(TimeOnly.AddHours)
                ? TimeSpan.TicksPerHour
                : TimeSpan.TicksPerMinute;
            var delta = _sqlExpressionFactory.Multiply(
                arguments[0], _sqlExpressionFactory.Constant((double)ticksPerUnit));
            var ticks = _sqlExpressionFactory.Case(
                [
                    new CaseWhenClause(
                        _sqlExpressionFactory.Function(
                            "Math::IsNaN", [delta], nullable: true, ArrayUtil.TrueArrays[1], typeof(bool)),
                        _sqlExpressionFactory.Constant(0L)),
                    new CaseWhenClause(
                        _sqlExpressionFactory.GreaterThanOrEqual(
                            delta, _sqlExpressionFactory.Constant((double)long.MaxValue)),
                        _sqlExpressionFactory.Constant(long.MaxValue)),
                    new CaseWhenClause(
                        _sqlExpressionFactory.LessThanOrEqual(
                            delta, _sqlExpressionFactory.Constant((double)long.MinValue)),
                        _sqlExpressionFactory.Constant(long.MinValue))
                ],
                _sqlExpressionFactory.Convert(delta, typeof(long)));
            var day = _sqlExpressionFactory.Constant(TimeSpan.TicksPerDay);

            return _sqlExpressionFactory.Convert(
                _sqlExpressionFactory.Modulo(
                    _sqlExpressionFactory.Add(
                        _sqlExpressionFactory.Add(
                            _sqlExpressionFactory.Convert(
                                _sqlExpressionFactory.ApplyTypeMapping(instance, timeMapping), typeof(long)),
                            day),
                        _sqlExpressionFactory.Modulo(ticks, day)),
                    day),
                typeof(TimeOnly),
                timeMapping);
        }

        if (instance is not null
            && method.DeclaringType == typeof(DateOnly)
            && method.Name == nameof(DateOnly.ToDateTime)
            && arguments.Count == 1)
        {
            if (arguments[0].TypeMapping is { Converter: not TimeOnlyToTicksConverter })
            {
                return null;
            }

            var timestampMapping = _typeMappingSource.FindMapping(typeof(DateTime), "Timestamp64");
            var timeMapping = (RelationalTypeMapping)_typeMappingSource.FindMapping(typeof(long))!
                .WithComposedConverter(new TimeOnlyToTicksConverter());

            return _sqlExpressionFactory.Add(
                _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.ApplyTypeMapping(
                        instance,
                        _typeMappingSource.FindMapping(typeof(DateOnly), "Date32")),
                    typeof(DateTime),
                    timestampMapping),
                _sqlExpressionFactory.Function(
                    "DateTime::Interval64FromMicroseconds",
                    [
                        _sqlExpressionFactory.Divide(
                            _sqlExpressionFactory.Convert(
                                _sqlExpressionFactory.ApplyTypeMapping(arguments[0], timeMapping),
                                typeof(long)),
                            _sqlExpressionFactory.Constant(TimeSpan.TicksPerMicrosecond))
                    ],
                    nullable: true,
                    ArrayUtil.TrueArrays[1],
                    typeof(TimeSpan),
                    _typeMappingSource.FindMapping(typeof(TimeSpan), "Interval64")),
                timestampMapping);
        }

        if (method.DeclaringType == typeof(DateOnly) && method.Name == nameof(DateOnly.FromDayNumber))
        {
            return _sqlExpressionFactory.Convert(
                _sqlExpressionFactory.Subtract(
                    arguments[0],
                    _sqlExpressionFactory.Constant(new DateOnly(1970, 1, 1).DayNumber)),
                typeof(DateOnly),
                _typeMappingSource.FindMapping(typeof(DateOnly), "Date32"));
        }

        if (method.DeclaringType == typeof(DateOnly) && method.Name == nameof(DateOnly.FromDateTime))
        {
            var dateType = arguments[0].TypeMapping is IYdbTypeMapping
            {
                YdbDbType: YdbDbType.Date32 or YdbDbType.Datetime64 or YdbDbType.Timestamp64
            }
                ? "Date32"
                : "Date";

            return _sqlExpressionFactory.Function(
                "DateTime::Make" + dateType,
                [arguments[0]],
                nullable: true,
                ArrayUtil.TrueArrays[1],
                typeof(DateOnly),
                _typeMappingSource.FindMapping(typeof(DateOnly), dateType));
        }

        return TranslateDatePart(instance, method, arguments);
    }

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
            var isDateOnly = instance.Type == typeof(DateOnly);
            if (isDateOnly && datePart == "Days")
            {
                return _sqlExpressionFactory.Add(
                    instance,
                    _sqlExpressionFactory.Function(
                        instance.TypeMapping!.StoreType == "Date32"
                            ? "DateTime::Interval64FromDays"
                            : "DateTime::IntervalFromDays",
                        [arguments[0]],
                        nullable: true,
                        ArrayUtil.TrueArrays[1],
                        typeof(TimeSpan)),
                    instance.TypeMapping);
            }

            if (datePart is "Days" or "Hours" or "Mins" or "Secs")
            {
                var ticksPerUnit = datePart switch
                {
                    "Days" => TimeSpan.TicksPerDay,
                    "Hours" => TimeSpan.TicksPerHour,
                    "Mins" => TimeSpan.TicksPerMinute,
                    _ => TimeSpan.TicksPerSecond
                };
                var wholeUnits = _sqlExpressionFactory.Convert(arguments[0], typeof(long));
                var intervalType = instance.TypeMapping is IYdbTypeMapping
                {
                    YdbDbType: YdbDbType.Date32 or YdbDbType.Datetime64 or YdbDbType.Timestamp64
                }
                    ? "Interval64"
                    : "Interval";

                return _sqlExpressionFactory.Add(
                    instance,
                    _sqlExpressionFactory.Function(
                        "DateTime::" + intervalType + "FromMicroseconds",
                        [
                            _sqlExpressionFactory.Add(
                                _sqlExpressionFactory.Multiply(
                                    wholeUnits,
                                    _sqlExpressionFactory.Constant(ticksPerUnit / TimeSpan.TicksPerMicrosecond)),
                                _sqlExpressionFactory.Divide(
                                    _sqlExpressionFactory.Convert(
                                        _sqlExpressionFactory.Multiply(
                                            _sqlExpressionFactory.Function(
                                                "Math::Fmod",
                                                [arguments[0], _sqlExpressionFactory.Constant(1.0)],
                                                nullable: true,
                                                ArrayUtil.TrueArrays[2],
                                                typeof(double)),
                                            _sqlExpressionFactory.Constant((double)ticksPerUnit)),
                                        typeof(long)),
                                    _sqlExpressionFactory.Constant(TimeSpan.TicksPerMicrosecond)))
                        ],
                        nullable: true,
                        ArrayUtil.TrueArrays[1],
                        typeof(TimeSpan),
                        _typeMappingSource.FindMapping(typeof(TimeSpan), intervalType)),
                    instance.TypeMapping);
            }

            var shiftDatePartFunction = _sqlExpressionFactory.Function(
                "DateTime::Shift" + datePart,
                [instance, arguments[0]],
                nullable: true,
                ArrayUtil.TrueArrays[2],
                returnType: typeof(DateTime)
            );

            return _sqlExpressionFactory.Function(
                "DateTime::Make" + instance.TypeMapping!.StoreType,
                arguments: [shiftDatePartFunction],
                nullable: true,
                ArrayUtil.TrueArrays[1],
                returnType: instance.Type,
                typeMapping: instance.TypeMapping
            );
        }

        return null;
    }
}
