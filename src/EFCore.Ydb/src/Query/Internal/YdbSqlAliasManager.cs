using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbSqlAliasManager : SqlAliasManager
{
    public override Expression PostprocessAliases(Expression expression)
    {
        var aliasRewriter = new AliasRewriter();
        return base.PostprocessAliases(aliasRewriter.Visit(expression));
    }

    // TODO: Temporary solution to solve following problem (https://t.me/ydb_ru/28649)
    // Doesn't work in all cases. Should be improved
    private sealed class AliasRewriter : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node) => node switch
        {
            ShapedQueryExpression shapedQuery => shapedQuery.UpdateQueryExpression(Visit(shapedQuery.QueryExpression)),
            SelectExpression selectExpression => VisitSelect(selectExpression),
            _ => base.VisitExtension(node)
        };

        private Expression VisitSelect(SelectExpression selectExpression)
        {
            var newTables = new List<TableExpressionBase>(selectExpression.Tables.Count);
            foreach (var table in selectExpression.Tables)
            {
                // We cannot change type in current expressionVisitor. Only adjust aliases
                newTables.Add((Visit(table) as TableExpressionBase)!);
            }

            var newProjections = AdjustAliases(selectExpression.Projection);

            return selectExpression.Update(
                tables: newTables,
                predicate: selectExpression.Predicate,
                groupBy: selectExpression.GroupBy,
                having: selectExpression.Having,
                projections: newProjections,
                orderings: selectExpression.Orderings,
                offset: selectExpression.Offset,
                limit: selectExpression.Limit
            );
        }

        private Expression VisitTableBase(TableExpressionBase tableExpression)
            => tableExpression switch
            {
                TableExpression t => VisitTable(t),
                SelectExpression selectExpression => VisitSelect(selectExpression),
                LeftJoinExpression leftJoinExpression => VisitLeftJoin(leftJoinExpression),
                _ => Visit(tableExpression)
            };

        private Expression VisitLeftJoin(LeftJoinExpression leftJoinExpression)
            => leftJoinExpression.Update(
                (VisitTableBase(leftJoinExpression.Table) as TableExpressionBase)!,
                leftJoinExpression.JoinPredicate
            );

        private Expression VisitTable(TableExpressionBase tableExpression)
        {
            return tableExpression;
        }

        private IReadOnlyList<ProjectionExpression> AdjustAliases(IReadOnlyList<ProjectionExpression> projections)
        {
            var newProjections = new ProjectionExpression[projections.Count];
            var knownAliases = new Dictionary<string, int>();
            var isTrueAlias = new bool[projections.Count];

            for (var i = 0; i < projections.Count; i++)
            {
                if (projections[i].Alias != string.Empty)
                {
                    if (projections[i].Expression is ColumnExpression columnExpression &&
                        columnExpression.Name == projections[i].Alias)
                    {
                        isTrueAlias[i] = false;
                        knownAliases.TryAdd(projections[i].Alias, i);
                    }
                    else
                    {
                        isTrueAlias[i] = true;
                        knownAliases[projections[i].Alias] = i;
                    }
                }
                else
                {
                    isTrueAlias[i] = false;
                }
            }

            for (var i = 0; i < projections.Count; i++)
            {
                if (isTrueAlias[i])
                {
                    newProjections[i] = projections[i];
                    continue;
                }

                var currentProjection = projections[i];
                int? key = knownAliases.TryGetValue(currentProjection.Alias, out var aliasPosition)
                    ? aliasPosition
                    : null;

                if (key == i)
                {
                    newProjections[i] = currentProjection;
                }
                else
                {
                    newProjections[i] =
                        new ProjectionExpression(currentProjection.Expression, alias: string.Empty);
                }
            }

            return newProjections;
        }
    }
}
