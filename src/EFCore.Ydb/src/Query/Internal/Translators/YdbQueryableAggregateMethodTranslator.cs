using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using EntityFrameworkCore.Ydb.Utilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal.Translators;

public class YdbQueryableAggregateMethodTranslator(
    YdbSqlExpressionFactory sqlExpressionFactory,
    IRelationalTypeMappingSource typeMappingSource)
    : IAggregateMethodCallTranslator
{
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
            case nameof(Queryable.Average) when
                (QueryableMethods.IsAverageWithoutSelector(methodInfo) ||
                 QueryableMethods.IsAverageWithSelector(methodInfo))
                && source.Selector is SqlExpression averageSqlExpression:
                var averageInputType = averageSqlExpression.Type;
                if (averageInputType == typeof(int)
                    || (averageInputType == typeof(long) && !source.IsDistinct))
                {
                    averageSqlExpression = sqlExpressionFactory.ApplyDefaultTypeMapping(
                        sqlExpressionFactory.Convert(averageSqlExpression, typeof(double)));
                }

                averageSqlExpression = CombineTerms(source, averageSqlExpression);
                return averageInputType == typeof(float)
                    ? sqlExpressionFactory.Convert(
                        sqlExpressionFactory.Function(
                            "AVG",
                            [averageSqlExpression],
                            nullable: true,
                            argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                            returnType: typeof(double)),
                        averageSqlExpression.Type,
                        averageSqlExpression.TypeMapping)
                    : sqlExpressionFactory.Function(
                        "AVG",
                        [averageSqlExpression],
                        nullable: true,
                        argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                        averageInputType == typeof(long) && source.IsDistinct
                            ? typeof(double)
                            : averageSqlExpression.Type,
                        averageInputType == typeof(long) && source.IsDistinct
                            ? typeMappingSource.FindMapping(typeof(double))
                            : averageSqlExpression.TypeMapping);

            case nameof(Queryable.Count) when
                methodInfo == QueryableMethods.CountWithoutPredicate ||
                methodInfo == QueryableMethods.CountWithPredicate:
                var countSqlExpression = source.Selector as SqlExpression ?? sqlExpressionFactory.Fragment("*");
                return sqlExpressionFactory.Convert(
                    TranslateCount(source, countSqlExpression),
                    typeof(int),
                    typeMappingSource.FindMapping(typeof(int)));

            case nameof(Queryable.LongCount) when
                methodInfo == QueryableMethods.LongCountWithoutPredicate ||
                methodInfo == QueryableMethods.LongCountWithPredicate:
                var longCountSqlExpression = source.Selector as SqlExpression ?? sqlExpressionFactory.Fragment("*");
                return sqlExpressionFactory.Convert(
                    TranslateCount(source, longCountSqlExpression),
                    typeof(long),
                    typeMappingSource.FindMapping(typeof(long)));

            case nameof(Queryable.Max) when
                (methodInfo == QueryableMethods.MaxWithoutSelector || methodInfo == QueryableMethods.MaxWithSelector)
                && source.Selector is SqlExpression maxSqlExpression:
                maxSqlExpression = CombineTerms(source, maxSqlExpression);
                return sqlExpressionFactory.Function(
                    "MAX",
                    [maxSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                    maxSqlExpression.Type,
                    maxSqlExpression.TypeMapping);

            case nameof(Queryable.Min) when
                (methodInfo == QueryableMethods.MinWithoutSelector || methodInfo == QueryableMethods.MinWithSelector)
                && source.Selector is SqlExpression minSqlExpression:
                minSqlExpression = CombineTerms(source, minSqlExpression);
                return sqlExpressionFactory.Function(
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
                sumSqlExpression = CombineTerms(source, sumSqlExpression);
                var sumInputType = sumSqlExpression.Type;

                if (sumInputType == typeof(int))
                {
                    return sqlExpressionFactory.Convert(
                        sqlExpressionFactory.Function(
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
                    return sqlExpressionFactory.Convert(
                        sqlExpressionFactory.Function(
                            "SUM",
                            [sumSqlExpression],
                            nullable: true,
                            argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                            typeof(long)),
                        sumInputType,
                        sumSqlExpression.TypeMapping);
                }

                return sqlExpressionFactory.Function(
                    "SUM",
                    [sumSqlExpression],
                    nullable: true,
                    argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                    sumInputType,
                    sumSqlExpression.TypeMapping);
        }

        return null;
    }

    private SqlExpression TranslateCount(EnumerableExpression source, SqlExpression sqlExpression)
    {
        var count = sqlExpressionFactory.Function(
            "COUNT",
            [CombineTerms(source, sqlExpression)],
            nullable: false,
            argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
            typeof(long));
        if (!source.IsDistinct)
        {
            return count;
        }

        // SQL COUNT ignores null; LINQ DISTINCT keeps one null among the selected inputs.
        var predicate = sqlExpressionFactory.IsNull(sqlExpression);
        if (source.Predicate != null)
        {
            predicate = sqlExpressionFactory.AndAlso(source.Predicate, predicate);
        }

        return sqlExpressionFactory.Add(
            sqlExpressionFactory.Convert(count, typeof(long), typeMappingSource.FindMapping(typeof(long))),
            sqlExpressionFactory.Case(
                [
                    new CaseWhenClause(
                        sqlExpressionFactory.GreaterThan(
                            sqlExpressionFactory.Function(
                                "COUNT_IF",
                                [predicate],
                                nullable: false,
                                argumentsPropagateNullability: ArrayUtil.FalseArrays[1],
                                typeof(long)),
                            sqlExpressionFactory.Constant(0L)),
                        sqlExpressionFactory.Constant(1L))
                ],
                sqlExpressionFactory.Constant(0L)));
    }

    private SqlExpression CombineTerms(EnumerableExpression source, SqlExpression sqlExpression)
    {
        if (source.Predicate != null)
        {
            if (sqlExpression is SqlFragmentExpression)
            {
                sqlExpression = sqlExpressionFactory.Constant(1);
            }

            sqlExpression = sqlExpressionFactory.Case(
                [new CaseWhenClause(source.Predicate, sqlExpression)],
                elseResult: null);
        }

        return source.IsDistinct ? new DistinctExpression(sqlExpression) : sqlExpression;
    }
}
