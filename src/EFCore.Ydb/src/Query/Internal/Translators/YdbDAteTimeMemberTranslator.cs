using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using EntityFrameworkCore.Ydb.Storage.Internal;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbDateTimeMemberTranslator : IMemberTranslator
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly YdbSqlExpressionFactory _sqlExpressionFactory;
    private readonly RelationalTypeMapping _timestampMapping;

    public YdbDateTimeMemberTranslator(
        IRelationalTypeMappingSource typeMappingSource,
        YdbSqlExpressionFactory sqlExpressionFactory
    )
    {
        _typeMappingSource = typeMappingSource;
        _timestampMapping = typeMappingSource.FindMapping(typeof(DateTime), "TimeStamp")!;
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public virtual SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        var declaringType = member.DeclaringType;

        if (declaringType == typeof(TimeOnly))
        {
            throw new InvalidOperationException("Ydb doesn't support TimeOnly right now");
        }

        if (declaringType != typeof(DateTime) && declaringType != typeof(DateOnly))
        {
            return null;
        }

        if (member.Name == nameof(DateTime.Date))
        {
            switch (instance)
            {
                case { TypeMapping: YdbDateTimeTypeMapping }:
                case { Type: var type } when type == typeof(DateTime):
                    return _sqlExpressionFactory.Convert(
                        _sqlExpressionFactory.Convert(instance, typeof(DateOnly)),
                        typeof(DateTime)
                    );
                case { TypeMapping: YdbDateOnlyTypeMapping }:
                case { Type: var type } when type == typeof(DateOnly):
                    return instance;
                default:
                    return null;
            }
        }

        return member.Name switch
        {
            // TODO: Find out how to add
            // nameof(DateTime.Now) => ???,
            // nameof(DateTime.Today) => ???

            nameof(DateTime.UtcNow) => UtcNow(),

            nameof(DateTime.Year) => DatePart(instance!, "GetYear"),
            nameof(DateTime.Month) => DatePart(instance!, "GetMonth"),
            nameof(DateTime.Day) => DatePart(instance!, "GetDayOfMonth"),
            nameof(DateTime.Hour) => DatePart(instance!, "GetHour"),
            nameof(DateTime.Minute) => DatePart(instance!, "GetMinute"),
            nameof(DateTime.Second) => DatePart(instance!, "GetSecond"),
            nameof(DateTime.Millisecond) => DatePart(instance!, "GetMillisecondOfSecond"),

            nameof(DateTime.DayOfYear) => DatePart(instance!, "GetDayOfYear"),
            nameof(DateTime.DayOfWeek) => DatePart(instance!, "GetDayOfWeek"),

            // TODO: Research if it's possible to implement
            nameof(DateTime.Ticks) => null,
            _ => null
        };

        SqlExpression UtcNow()
            => _sqlExpressionFactory.Function(
                "CurrentUtc" + returnType.Name == "DateOnly" ? "Date" : returnType.Name,
                [],
                nullable: false,
                argumentsPropagateNullability: ArrayUtil.TrueArrays[0],
                returnType,
                _typeMappingSource.FindMapping(returnType)
            );
    }

    private SqlExpression? DatePart(SqlExpression instance, string partName)
    {
        var result = _sqlExpressionFactory.Function(
            $"DateTime::{partName}",
            [instance],
            nullable: true,
            argumentsPropagateNullability: ArrayUtil.TrueArrays[1],
            typeof(short) // Doesn't matter because we cast it to int in next line anyway
        );

        return _sqlExpressionFactory.Convert(result, typeof(int));
    }
}
