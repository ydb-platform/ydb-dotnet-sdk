using System;
using System.Reflection;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public sealed class YdbDateTimeMemberTranslator(
    IRelationalTypeMappingSource typeMappingSource,
    YdbSqlExpressionFactory sqlExpressionFactory)
    : IMemberTranslator
{
    public SqlExpression? Translate(
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

        if (member.Name == nameof(DateTime.Date) && instance is not null)
        {
            if (instance.TypeMapping is IYdbTypeMapping { YdbDbType: YdbDbType.Date or YdbDbType.Date32 })
            {
                return instance;
            }

            return sqlExpressionFactory.Function(
                "DateTime::Make" + instance.TypeMapping!.StoreType,
                [
                    sqlExpressionFactory.Function(
                        "DateTime::StartOfDay",
                        [instance],
                        nullable: true,
                        ArrayUtil.TrueArrays[1],
                        returnType)
                ],
                nullable: true,
                ArrayUtil.TrueArrays[1],
                returnType,
                instance.TypeMapping);
        }

        return member.Name switch
        {
            // TODO: Find out how to add
            // nameof(DateTime.Now) => ???,
            // nameof(DateTime.Today) => ???

            nameof(DateTime.UtcNow) => sqlExpressionFactory.Function(
                "CurrentUtcTimestamp",
                [],
                nullable: false,
                argumentsPropagateNullability: ArrayUtil.TrueArrays[0],
                returnType,
                typeMappingSource.FindMapping(returnType)
            ),
            nameof(DateTime.Year) => DatePart(instance!, "GetYear"),
            nameof(DateTime.Month) => DatePart(instance!, "GetMonth"),
            nameof(DateTime.Day) => DatePart(instance!, "GetDayOfMonth"),
            nameof(DateTime.Hour) => DatePart(instance!, "GetHour"),
            nameof(DateTime.Minute) => DatePart(instance!, "GetMinute"),
            nameof(DateTime.Second) => DatePart(instance!, "GetSecond"),
            nameof(DateTime.Millisecond) => DatePart(instance!, "GetMillisecondOfSecond"),
            nameof(DateTime.TimeOfDay) => sqlExpressionFactory.Function(
                "DateTime::TimeOfDay",
                [instance!],
                nullable: true,
                argumentsPropagateNullability: ArrayUtil.TrueArrays[1],
                returnType,
                typeMappingSource.FindMapping(
                    returnType,
                    instance!.TypeMapping is IYdbTypeMapping
                    {
                        YdbDbType: YdbDbType.Date32 or YdbDbType.Datetime64 or YdbDbType.Timestamp64
                    }
                        ? "Interval64"
                        : "Interval")),

            nameof(DateTime.DayOfYear) => DatePart(instance!, "GetDayOfYear"),
            nameof(DateTime.DayOfWeek) => sqlExpressionFactory.Modulo(
                DatePart(instance!, "GetDayOfWeek"),
                sqlExpressionFactory.Constant(7)),
            nameof(DateOnly.DayNumber) => sqlExpressionFactory.Add(
                sqlExpressionFactory.Convert(instance!, typeof(int)),
                sqlExpressionFactory.Constant(new DateOnly(1970, 1, 1).DayNumber)),

            // TODO: Research if it's possible to implement
            nameof(DateTime.Ticks) => null,
            _ => null
        };
    }

    private SqlExpression DatePart(SqlExpression instance, string partName)
    {
        var result = sqlExpressionFactory.Function(
            $"DateTime::{partName}",
            [instance],
            nullable: true,
            argumentsPropagateNullability: ArrayUtil.TrueArrays[1],
            typeof(short) // Doesn't matter because we cast it to int in next line anyway
        );

        return sqlExpressionFactory.Convert(result, typeof(int));
    }
}
