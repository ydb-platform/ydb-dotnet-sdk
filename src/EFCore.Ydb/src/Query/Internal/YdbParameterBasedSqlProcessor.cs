using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters
) : RelationalParameterBasedSqlProcessor(dependencies, parameters)
{
#if EFCORE9
    protected override Expression ProcessSqlNullability(Expression queryExpression,
        IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        => new GroupByAggregateRewriter().Visit(
            new JoinPredicateRewriter(Dependencies.SqlExpressionFactory).Visit(
                new YdbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersValues,
                    out canCache)));
#else
    protected override Expression ProcessSqlNullability(Expression queryExpression,
        ParametersCacheDecorator parametersDecorator)
        => new GroupByAggregateRewriter().Visit(
            new JoinPredicateRewriter(Dependencies.SqlExpressionFactory).Visit(
                new YdbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression,
                    parametersDecorator)));
#endif

    private sealed class GroupByAggregateRewriter : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
        {
            var visited = base.VisitExtension(node);
            if (visited is not SelectExpression select
                || select.GroupBy.Count == 0
                || select.GroupBy.Any(key => key is not ColumnExpression))
            {
                return visited;
            }

            var rewriter = new AggregateRewriter(select);
            var having = (SqlExpression?)rewriter.Visit(select.Having);
            ProjectionExpression[]? projections = null;
            for (var i = 0; i < select.Projection.Count; i++)
            {
                var projection = (ProjectionExpression)rewriter.Visit(select.Projection[i]);
                if (ReferenceEquals(projection, select.Projection[i]))
                {
                    continue;
                }

                projections ??= select.Projection.ToArray();
                projections[i] = projection;
            }

            return ReferenceEquals(having, select.Having) && projections is null
                ? select
                : select.Update(rewriter.Tables, select.Predicate, select.GroupBy, having,
                    projections ?? select.Projection,
                    select.Orderings, select.Offset, select.Limit);
        }

        private sealed class AggregateRewriter(SelectExpression outer) : ExpressionVisitor
        {
            public IReadOnlyList<TableExpressionBase> Tables { get; private set; } = outer.Tables;

            protected override Expression VisitExtension(Expression node)
            {
                if (node is ScalarSubqueryExpression scalar)
                {
                    return TryInline(scalar.Subquery) ?? scalar;
                }

                return node is SelectExpression ? node : base.VisitExtension(node);
            }

            private SqlExpression? TryInline(SelectExpression inner)
            {
                if (inner.IsDistinct || inner.GroupBy.Count != 0 || inner.Having is not null
                    || inner.Orderings.Count != 0 || inner.Offset is not null || inner.Limit is not null
                    || inner.Projection.Count != 1
                    || inner.Projection[0].Expression is not SqlFunctionExpression
                    {
                        Name: "MIN" or "MAX", IsBuiltIn: true, Arguments: [_]
                    } aggregate
                    || outer.Tables.Count == 0 || inner.Tables.Count != outer.Tables.Count + 1
                    || outer.Tables[0] is not TableExpression outerRoot
                    || inner.Tables[0] is not TableExpression innerRoot
                    || outerRoot.Table != innerRoot.Table)
                {
                    return null;
                }

                var aliases = new Dictionary<string, string> { [innerRoot.Alias] = outerRoot.Alias };
                for (var i = 1; i < outer.Tables.Count; i++)
                {
                    if (outer.Tables[i] is not PredicateJoinExpressionBase
                        {
                            Table: TableExpression outerTable
                        } outerJoin
                        || inner.Tables[i] is not PredicateJoinExpressionBase
                        {
                            Table: TableExpression innerTable
                        } innerJoin
                        || outerJoin is not (InnerJoinExpression or LeftJoinExpression)
                        || outerJoin.GetType() != innerJoin.GetType()
                        || outerTable.Table != innerTable.Table)
                    {
                        return null;
                    }

                    aliases.Add(innerTable.Alias, outerTable.Alias);
                    var mapper = new ColumnAliasRewriter(aliases, true);
                    var predicate = mapper.Visit(innerJoin.JoinPredicate);
                    if (!mapper.IsValid || !predicate.Equals(outerJoin.JoinPredicate))
                    {
                        return null;
                    }
                }

                if (inner.Tables[^1] is not PredicateJoinExpressionBase { Table: TableExpression extraTable } extraJoin
                    || extraJoin is not (InnerJoinExpression or LeftJoinExpression))
                {
                    return null;
                }

                // Repeating the same primary-key lookup also preserves a LEFT join's null extension.
                var matched = false;
                foreach (var candidate in Tables.OfType<PredicateJoinExpressionBase>())
                {
                    var table = (TableExpression)candidate.Table;
                    if (candidate.GetType() != extraJoin.GetType() || table.Table != extraTable.Table
                                                                   || !JoinsPrimaryKey(candidate.JoinPredicate, table))
                    {
                        continue;
                    }

                    aliases[extraTable.Alias] = table.Alias;
                    var mapper = new ColumnAliasRewriter(aliases, true);
                    var predicate = mapper.Visit(extraJoin.JoinPredicate);
                    if (mapper.IsValid && predicate.Equals(candidate.JoinPredicate))
                    {
                        matched = true;
                        break;
                    }
                }

                LeftJoinExpression? liftedJoin = null;
                if (!matched)
                {
                    if (aggregate.Name != "MAX" || extraJoin is not LeftJoinExpression leftJoin
                                                || !JoinsPrimaryKey(extraJoin.JoinPredicate, extraTable)
                                                || Tables.Any(table =>
                                                    (table is JoinExpressionBase join
                                                        ? join.Table.Alias
                                                        : table.Alias) == extraTable.Alias))
                    {
                        return null;
                    }

                    // A complete primary-key LEFT lookup adds exactly one row, including missing matches.
                    aliases[extraTable.Alias] = extraTable.Alias;
                    var mapper = new ColumnAliasRewriter(aliases, true);
                    var predicate = (SqlExpression)mapper.Visit(extraJoin.JoinPredicate);
                    if (!mapper.IsValid)
                    {
                        return null;
                    }

                    liftedJoin = leftJoin.Update(extraTable, predicate);
                }

                var predicateMapper = new ColumnAliasRewriter(aliases, false);
                var terms = Conjuncts((SqlExpression?)predicateMapper.Visit(inner.Predicate)).ToList();
                if (!predicateMapper.IsValid)
                {
                    return null;
                }

                foreach (var outerTerm in Conjuncts(outer.Predicate))
                {
                    if (!terms.Remove(outerTerm))
                    {
                        return null;
                    }
                }

                foreach (var key in outer.GroupBy.Cast<ColumnExpression>())
                {
                    var index = terms.FindIndex(term => IsCorrelation(term, key));
                    if (index < 0)
                    {
                        return null;
                    }

                    terms.RemoveAt(index);
                }

                if (terms.Count != 0)
                {
                    return null;
                }

                var aggregateMapper = new ColumnAliasRewriter(aliases, true);
                var rewritten = (SqlExpression)aggregateMapper.Visit(aggregate);
                if (!aggregateMapper.IsValid)
                {
                    return null;
                }

                if (liftedJoin is not null)
                {
                    Tables = Tables.Append(liftedJoin).ToArray();
                }

                return rewritten;
            }

            private static bool JoinsPrimaryKey(SqlExpression predicate, TableExpression table)
            {
                if (table.Table is not ITable { PrimaryKey: { } primaryKey })
                {
                    return false;
                }

                var equalities = Conjuncts(predicate).OfType<SqlBinaryExpression>()
                    .Where(expression => expression.OperatorType == ExpressionType.Equal).ToArray();
                return primaryKey.Columns.All(key => equalities.Any(equality =>
                    equality.Left is ColumnExpression left && equality.Right is ColumnExpression right
                                                           && ((left.TableAlias == table.Alias &&
                                                                left.Name == key.Name &&
                                                                right.TableAlias != table.Alias)
                                                               || (right.TableAlias == table.Alias &&
                                                                   right.Name == key.Name &&
                                                                   left.TableAlias != table.Alias))));
            }

            private static IEnumerable<SqlExpression> Conjuncts(SqlExpression? expression)
            {
                if (expression is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } conjunction)
                {
                    foreach (var term in Conjuncts(conjunction.Left))
                    {
                        yield return term;
                    }

                    foreach (var term in Conjuncts(conjunction.Right))
                    {
                        yield return term;
                    }
                }
                else if (expression is not null)
                {
                    yield return expression;
                }
            }

            private static bool IsCorrelation(SqlExpression expression, ColumnExpression key)
            {
                if (!key.IsNullable && IsEquality(expression, key))
                {
                    return true;
                }

                return expression is SqlBinaryExpression
                       {
                           OperatorType: ExpressionType.OrElse, Left: var left, Right: var right
                       }
                       && ((IsEquality(left, key) && IsNullPair(right, key))
                           || (IsEquality(right, key) && IsNullPair(left, key)));
            }

            private static bool IsEquality(SqlExpression expression, ColumnExpression key) =>
                expression is SqlBinaryExpression { OperatorType: ExpressionType.Equal } equality
                && equality.Left.Equals(key) && equality.Right.Equals(key);

            private static bool IsNullPair(SqlExpression expression, ColumnExpression key) =>
                expression is SqlBinaryExpression { OperatorType: ExpressionType.AndAlso } conjunction
                && IsNull(conjunction.Left, key) && IsNull(conjunction.Right, key);

            private static bool IsNull(SqlExpression expression, ColumnExpression key) =>
                expression is SqlUnaryExpression { OperatorType: ExpressionType.Equal } unary
                && unary.Operand.Equals(key);
        }

        private sealed class ColumnAliasRewriter(
            IReadOnlyDictionary<string, string> aliases,
            bool requireMappedColumns) : ExpressionVisitor
        {
            public bool IsValid { get; private set; } = true;

            protected override Expression VisitExtension(Expression node)
            {
                if (node is ColumnExpression column)
                {
                    if (aliases.TryGetValue(column.TableAlias, out var alias))
                    {
#if EFCORE9
                        return new ColumnExpression(column.Name, alias, column.Type, column.TypeMapping,
                            column.IsNullable);
#else
                        return new ColumnExpression(column.Name, alias, column.Column, column.Type, column.TypeMapping,
                            column.IsNullable);
#endif
                    }

                    if (requireMappedColumns)
                    {
                        IsValid = false;
                    }

                    return column;
                }

                if (node is SelectExpression or ScalarSubqueryExpression)
                {
                    IsValid = false;
                    return node;
                }

                return base.VisitExtension(node);
            }
        }
    }

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
