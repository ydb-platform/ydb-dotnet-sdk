using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
        private bool isRootSelect = true;

        [return: NotNullIfNotNull("node")]
        public override Expression? Visit(Expression? node) => node switch
        {
            ShapedQueryExpression shapedQuery => shapedQuery.UpdateQueryExpression(Visit(shapedQuery.QueryExpression)),
            SelectExpression selectExpression => VisitSelect(selectExpression),

            _ => base.Visit(node)
        };

        protected override Expression VisitExtension(Expression node) => node switch
        {
            ShapedQueryExpression shapedQuery => shapedQuery.UpdateQueryExpression(Visit(shapedQuery.QueryExpression)),
            SelectExpression selectExpression => VisitSelect(selectExpression),
            _ => base.VisitExtension(node)
        };

        private Expression VisitSelect(SelectExpression selectExpression)
        {
            var newProjections = AdjustAliases(selectExpression.Projection, isRootSelect);
            isRootSelect = false;

            var newTables = new List<TableExpressionBase>(selectExpression.Tables.Count);
            foreach (var table in selectExpression.Tables)
            {
                // We cannot change type in current expressionVisitor. Only adjust aliases
                newTables.Add((Visit(table) as TableExpressionBase)!);
            }

            var news = selectExpression.Update(
                tables: newTables,
                predicate: selectExpression.Predicate,
                groupBy: selectExpression.GroupBy,
                having: selectExpression.Having,
                projections: newProjections,
                orderings: selectExpression.Orderings,
                offset: selectExpression.Offset,
                limit: selectExpression.Limit
            );
            return news;
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

        private Expression VisitTable(TableExpressionBase tableExpression) => tableExpression;

        private IReadOnlyList<ProjectionExpression> AdjustAliases(
            IReadOnlyList<ProjectionExpression> projections,
            bool isRoot
        )
        {
            var newProjections = new ProjectionExpression[projections.Count];

            if (isRoot)
            {
                for (var i = 0; i < projections.Count; i++)
                {
                    var currentProjection = projections[i];
                    if (currentProjection.Expression is not ColumnExpression columnExpression)
                    {
                        newProjections[i] = currentProjection;
                    }
                    else
                    {
                        newProjections[i] =
                            currentProjection.Alias == columnExpression.Name
                                ? new ProjectionExpression(currentProjection.Expression, string.Empty)
                                : currentProjection;
                    }
                }

                return newProjections;
            }

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
