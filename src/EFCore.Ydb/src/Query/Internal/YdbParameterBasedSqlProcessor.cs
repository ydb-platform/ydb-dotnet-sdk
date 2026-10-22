using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
#if EFCORE9
using System.Collections.Generic;
#endif

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters
) : RelationalParameterBasedSqlProcessor(dependencies, parameters)
{
#if EFCORE9
    protected override Expression ProcessSqlNullability(Expression queryExpression,
        IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        => new JoinPredicateRewriter(Dependencies.SqlExpressionFactory).Visit(
            new YdbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersValues,
                out canCache));
#else
    protected override Expression ProcessSqlNullability(Expression queryExpression,
        ParametersCacheDecorator parametersDecorator)
        => new JoinPredicateRewriter(Dependencies.SqlExpressionFactory).Visit(
            new YdbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersDecorator));
#endif

    private sealed class JoinPredicateRewriter(ISqlExpressionFactory sqlExpressionFactory) : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
        {
            var visited = base.VisitExtension(node);
            if (visited is not SelectExpression select)
            {
                return visited;
            }

            select = RewriteApplies(select);
            TableExpressionBase[]? tables = null;
            var predicate = select.Predicate;
            for (var i = 0; i < select.Tables.Count; i++)
            {
                if (select.Tables[i] is not InnerJoinExpression join)
                {
                    continue;
                }

#if !EFCORE9
                // A later RIGHT JOIN can preserve rows rejected by this predicate.
                if (select.Tables.Skip(i + 1).Any(table => table is RightJoinExpression))
                {
                    continue;
                }
#endif

                SqlExpression? residual = null;
                var keys = ExtractKeys(join.JoinPredicate, join.Table.Alias, ref residual);
                if (residual is null)
                {
                    continue;
                }

                tables ??= select.Tables.ToArray();
                tables[i] = keys is null
                    ? new CrossJoinExpression(join.Table)
                    : join.Update(join.Table, keys);
                predicate = predicate is null ? residual : sqlExpressionFactory.AndAlso(predicate, residual);
            }

            return tables is null
                ? select
                : select.Update(tables, predicate, select.GroupBy, select.Having, select.Projection,
                    select.Orderings, select.Offset, select.Limit);
        }

        private SelectExpression RewriteApplies(SelectExpression select)
        {
            for (var i = 0; i < select.Tables.Count; i++)
            {
                if (select.Tables[i] is not CrossApplyExpression
                    {
                        Table: SelectExpression
                        {
                            IsDistinct: false, GroupBy.Count: 0, Having: null, Orderings.Count: 0,
                            Offset: null, Limit: null
                        } inner
                    }
                    || inner.Tables is not [TableExpression source]
                    || inner.Projection.Any(projection => projection.Expression is not ColumnExpression))
                {
                    continue;
                }

#if !EFCORE9
                if (select.Tables.Skip(i + 1).Any(table => table is RightJoinExpression))
                {
                    continue;
                }
#endif

                var tables = select.Tables.ToArray();
                tables[i] = new CrossJoinExpression(source);
                var predicate = inner.Predicate is null
                    ? select.Predicate
                    : select.Predicate is null
                        ? inner.Predicate
                        : sqlExpressionFactory.AndAlso(select.Predicate, inner.Predicate);
                select = select.Update(tables, predicate, select.GroupBy, select.Having, select.Projection,
                    select.Orderings, select.Offset, select.Limit);
                select = (SelectExpression)new ApplyProjectionRewriter(inner).Visit(select);
            }

            return select;
        }

        private SqlExpression? ExtractKeys(SqlExpression predicate, string? joinedAlias, ref SqlExpression? residual)
        {
            if (predicate is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } conjunction)
            {
                var left = ExtractKeys(conjunction.Left, joinedAlias, ref residual);
                var right = ExtractKeys(conjunction.Right, joinedAlias, ref residual);
                return left is null ? right : right is null ? left : sqlExpressionFactory.AndAlso(left, right);
            }

            if (predicate is SqlBinaryExpression
                {
                    OperatorType: ExpressionType.Equal,
                    Left: ColumnExpression leftColumn,
                    Right: ColumnExpression rightColumn
                }
                && leftColumn.TableAlias == joinedAlias != (rightColumn.TableAlias == joinedAlias))
            {
                return predicate;
            }

            residual = residual is null ? predicate : sqlExpressionFactory.AndAlso(residual, predicate);
            return null;
        }

        private sealed class ApplyProjectionRewriter(SelectExpression inner) : ExpressionVisitor
        {
            protected override Expression VisitExtension(Expression node) => node switch
            {
                ColumnExpression column when column.TableAlias == inner.Alias
                    => inner.Projection.Single(projection => projection.Alias == column.Name).Expression,
                _ => base.VisitExtension(node)
            };
        }
    }
}
