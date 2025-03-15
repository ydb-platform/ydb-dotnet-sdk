using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EfCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Query.Internal.Translators;

public class YdbQueryableAggregateMethodTranslator : IAggregateMethodCallTranslator
{
    private readonly YdbSqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public YdbQueryableAggregateMethodTranslator(
        YdbSqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource
    )
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger
    )
    {
        if (method.DeclaringType != typeof(Queryable)) return null;

        var methodInfo = method.IsGenericMethod
            ? method.GetGenericMethodDefinition()
            : method;
        switch (methodInfo.Name)
        {
            case nameof(Queryable.Average)
                when (QueryableMethods.IsAverageWithoutSelector(methodInfo)
                      || QueryableMethods.IsAverageWithSelector(methodInfo))
                     && source.Selector is SqlExpression averageSqlExpression:
                var averageInputType = averageSqlExpression.Type;
                if (averageInputType == typeof(int)
                    || averageInputType == typeof(long))
                {
                    averageSqlExpression = _sqlExpressionFactory.ApplyDefaultTypeMapping(
                        _sqlExpressionFactory.Convert(averageSqlExpression, typeof(double)));
                }

                return averageInputType == typeof(float)
                    ? _sqlExpressionFactory.Convert(
                        _sqlExpressionFactory.Function(
                            "AVG",
                            [averageSqlExpression],
                            nullable: true,
                            argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                            returnType: typeof(double)),
                        averageSqlExpression.Type,
                        averageSqlExpression.TypeMapping)
                    : _sqlExpressionFactory.Function(
                        "AVG",
                        [averageSqlExpression],
                        nullable: true,
                        argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                        averageSqlExpression.Type,
                        averageSqlExpression.TypeMapping);

            case nameof(Queryable.Count)
                when methodInfo == QueryableMethods.CountWithoutPredicate
                     || methodInfo == QueryableMethods.CountWithPredicate:
                var countSqlExpression = (source.Selector as SqlExpression) ?? _sqlExpressionFactory.Fragment("*");
                return _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Function(
                        "COUNT",
                        [countSqlExpression],
                        nullable: false,
                        argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                        typeof(long)),
                    typeof(int),
                    _typeMappingSource.FindMapping(typeof(int)));

            case nameof(Queryable.LongCount)
                when methodInfo == QueryableMethods.LongCountWithoutPredicate
                     || methodInfo == QueryableMethods.LongCountWithPredicate:
                var longCountSqlExpression = (source.Selector as SqlExpression) ?? _sqlExpressionFactory.Fragment("*");
                return _sqlExpressionFactory.Function(
                    "COUNT",
                    [longCountSqlExpression],
                    nullable: false,
                    argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                    typeof(long));

            case nameof(Queryable.Max)
                when (methodInfo == QueryableMethods.MaxWithoutSelector
                      || methodInfo == QueryableMethods.MaxWithSelector)
                     && source.Selector is SqlExpression maxSqlExpression:
                return _sqlExpressionFactory.Function(
                    "MAX",
                    [maxSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                    maxSqlExpression.Type,
                    maxSqlExpression.TypeMapping);

            case nameof(Queryable.Min)
                when (methodInfo == QueryableMethods.MinWithoutSelector
                      || methodInfo == QueryableMethods.MinWithSelector)
                     && source.Selector is SqlExpression minSqlExpression:
                return _sqlExpressionFactory.Function(
                    "MIN",
                    [minSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                    minSqlExpression.Type,
                    minSqlExpression.TypeMapping);

            case nameof(Queryable.Sum)
                when (QueryableMethods.IsSumWithoutSelector(methodInfo)
                      || QueryableMethods.IsSumWithSelector(methodInfo))
                     && source.Selector is SqlExpression sumSqlExpression:
                var sumInputType = sumSqlExpression.Type;

                if (sumInputType == typeof(int))
                {
                    return _sqlExpressionFactory.Convert(
                        _sqlExpressionFactory.Function(
                            "SUM",
                            [sumSqlExpression],
                            nullable: true,
                            argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                            typeof(long)),
                        sumInputType,
                        sumSqlExpression.TypeMapping);
                }

                if (sumInputType == typeof(long))
                {
                    return _sqlExpressionFactory.Convert(
                        _sqlExpressionFactory.Function(
                            "SUM",
                            [sumSqlExpression],
                            nullable: true,
                            argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                            typeof(decimal)),
                        sumInputType,
                        sumSqlExpression.TypeMapping);
                }

                return _sqlExpressionFactory.Function(
                    "SUM",
                    [sumSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                    sumInputType,
                    sumSqlExpression.TypeMapping);
        }

        return null;
    }
}
