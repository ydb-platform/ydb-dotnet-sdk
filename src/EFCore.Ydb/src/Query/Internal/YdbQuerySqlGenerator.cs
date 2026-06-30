using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using EntityFrameworkCore.Ydb.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public sealed class YdbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : QuerySqlGenerator(dependencies)
{
    private bool SkipAliases { get; set; }
    private ISqlGenerationHelper SqlGenerationHelper => Dependencies.SqlGenerationHelper;

    protected override Expression VisitExtension(Expression extensionExpression) => extensionExpression switch
    {
        YdbILikeExpression e => VisitILike(e),
        _ => base.VisitExtension(extensionExpression)
    };

    protected override Expression VisitColumn(ColumnExpression columnExpression)
    {
        if (SkipAliases)
        {
            Sql.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
        }
        else
        {
            base.VisitColumn(columnExpression);
        }

        return columnExpression;
    }

    protected override Expression VisitTable(TableExpression tableExpression)
    {
        if (SkipAliases)
        {
            Sql.Append(SqlGenerationHelper.DelimitIdentifier(tableExpression.Name, tableExpression.Schema));
        }
        else
        {
            base.VisitTable(tableExpression);
        }

        return tableExpression;
    }

    protected override Expression VisitDelete(DeleteExpression deleteExpression)
    {
        Sql.Append("DELETE FROM ");

        SkipAliases = true;
        Visit(deleteExpression.Table);
        SkipAliases = false;

        var select = deleteExpression.SelectExpression;

        if (IsDeleteOnSelect(select, deleteExpression.Table))
        {
            GenerateDeleteOn(deleteExpression);
        }
        else
        {
            GenerateSimpleWhere(select, skipAliases: true);
        }

        return deleteExpression;
    }

    private void GenerateDeleteOn(DeleteExpression deleteExpression)
    {
        var select = deleteExpression.SelectExpression;

        if (select.Predicate is InExpression { Subquery: not null } inExpression)
        {
            Sql.Append(" ON ");
            Visit(inExpression.Subquery);
            return;
        }

        GenerateDeleteOnSubquery(deleteExpression.Table, select);
    }

    private static bool IsDeleteOnSelect(SelectExpression select, TableExpression table) =>
        IsComplexSelect(select, table)
        || (select.Predicate is InExpression { Subquery: not null } inExpression
            && IsComplexInSubquery(inExpression.Subquery));

    private static bool IsComplexInSubquery(SelectExpression select) =>
        select.Offset != null
        || select.Limit != null
        || select.Having != null
        || select.Orderings.Count > 0
        || select.GroupBy.Count > 0
        || select.Tables.Count > 1;

    private void GenerateSimpleWhere(SelectExpression select, bool skipAliases)
    {
        var predicate = select.Predicate;
        if (predicate == null) return;

        Sql.AppendLine().Append("WHERE ");
        if (skipAliases) SkipAliases = true;
        Visit(predicate);
        if (skipAliases) SkipAliases = false;
    }

    private void GenerateDeleteOnSubquery(TableExpression table, SelectExpression select)
    {
        Sql.Append(" ON ").AppendLine().Append("SELECT ");

        var first = true;
        foreach (var keyColumn in FindKeyColumnsForDeleteOn(table))
        {
            if (!first)
            {
                Sql.Append(", ");
            }

            Visit(keyColumn);
            AppendSelectAlias(keyColumn.Name);
            first = false;
        }

        if (!TryGenerateWithoutWrappingSelect(select))
        {
            GenerateFrom(select);
            if (select.Predicate != null)
            {
                Sql.AppendLine().Append("WHERE ");
                Visit(select.Predicate);
            }

            GenerateOrderings(select);
            GenerateLimitOffset(select);
        }

        if (select.Alias != null)
        {
            Sql.AppendLine()
                .Append(")")
                .Append(AliasSeparator)
                .Append(SqlGenerationHelper.DelimitIdentifier(select.Alias));
        }
    }

    private void AppendSelectAlias(string alias)
        => Sql.Append(AliasSeparator).Append(SqlGenerationHelper.DelimitIdentifier(alias));

    private static ImmutableList<ColumnExpression> FindKeyColumnsForDeleteOn(TableExpression table)
    {
        var tableAlias = table.Alias;

        var entityType = table.Table.EntityTypeMappings
            .Select(m => m.TypeBase)
            .OfType<IEntityType>()
            .FirstOrDefault();

        if (entityType?.FindPrimaryKey() is not { } primaryKey)
        {
            throw new InvalidOperationException(
                $"Could not determine key columns for DELETE ON over `{table.Name}`.");
        }

        var storeObject = StoreObjectIdentifier.Table(table.Name, table.Schema);

        return primaryKey.Properties
            .Select(property =>
            {
                var columnName = property.GetColumnName(storeObject)
                                 ?? throw new InvalidOperationException(
                                     $"Could not determine key column name for `{property.Name}` on `{table.Name}`.");

                return new ColumnExpression(
                    columnName,
                    tableAlias,
                    property.ClrType,
                    property.GetRelationalTypeMapping(),
                    property.IsNullable);
            })
            .ToImmutableList();
    }

    private static bool IsComplexSelect(SelectExpression select, TableExpressionBase fromTable) =>
        select.Offset != null
        || select.Limit != null
        || select.Having != null
        || select.Orderings.Count > 0
        || select.GroupBy.Count > 0
        || select.Projection.Count > 0
        || select.Tables.Count > 1
        || !(select.Tables.Count == 1 && select.Tables[0].Equals(fromTable));

    protected override Expression VisitUpdate(UpdateExpression updateExpression)
    {
        SkipAliases = true;
        base.VisitUpdate(updateExpression);
        SkipAliases = false;

        return updateExpression;
    }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit == null && selectExpression.Offset == null)
        {
            return;
        }

        Sql.AppendLine().Append("LIMIT ");
        if (selectExpression.Limit != null)
        {
            Visit(selectExpression.Limit);
        }
        else
        {
            // We must specify number here because offset without limit leads to exception
            Sql.Append(int.MaxValue.ToString());
        }

        // ReSharper disable once InvertIf
        if (selectExpression.Offset != null)
        {
            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

    protected override string GetOperator(SqlBinaryExpression binaryExpression)
        => binaryExpression.OperatorType switch
        {
            ExpressionType.Add when binaryExpression.Type == typeof(string)
                                    || binaryExpression.Left.TypeMapping?.ClrType == typeof(string)
                                    || binaryExpression.Right.TypeMapping?.ClrType == typeof(string)
                => " || ",
            _ => base.GetOperator(binaryExpression)
        };

    protected override Expression VisitJsonScalar(JsonScalarExpression jsonScalarExpression)
    {
        Sql.Append("JSON_VALUE(");
        Visit(jsonScalarExpression.Json);
        Sql.Append(",");

        var path = jsonScalarExpression.Path;
        if (!path.Any())
        {
            return jsonScalarExpression;
        }

        Sql.Append("\"$.");
        for (var i = 0; i < path.Count; i++)
        {
            var pathSegment = path[i];
            var isFirst = i == 0;

            switch (pathSegment)
            {
                case { PropertyName: { } propertyName }:
                    Sql.Append(isFirst ? "" : ".")
                        .Append(Dependencies.SqlGenerationHelper.DelimitJsonPathElement(propertyName));
                    break;
                case { ArrayIndex: SqlConstantExpression arrayIndex }:
                    Sql.Append("[");
                    Visit(arrayIndex);
                    Sql.Append("]");
                    break;
                default:
                    throw new UnreachableException();
            }
        }

        Sql.Append("\")");
        return jsonScalarExpression;
    }

    protected override Expression VisitProjection(ProjectionExpression projectionExpression)
    {
        Visit(projectionExpression.Expression);

        if (projectionExpression.Alias != string.Empty)
        {
            Sql
                .Append(AliasSeparator)
                .Append(SqlGenerationHelper.DelimitIdentifier(projectionExpression.Alias));
        }

        return projectionExpression;
    }

    private Expression VisitILike(YdbILikeExpression likeExpression, bool negated = false)
    {
        Visit(likeExpression.Match);

        if (negated)
        {
            Sql.Append(" NOT");
        }

        Sql.Append(" ILIKE ");

        Visit(likeExpression.Pattern);

        if (likeExpression.EscapeChar is not null)
        {
            Sql.Append(" ESCAPE ");
            // For escape character, we don't need the 'u' suffix 
            if (likeExpression.EscapeChar is SqlConstantExpression { Value: string escapeValue })
            {
                Sql.Append($"'{escapeValue.Replace("'", "''")}'");
            }
            else
            {
                Visit(likeExpression.EscapeChar);
            }
        }

        return likeExpression;
    }

    protected override Expression VisitCase(CaseExpression caseExpression)
    {
        Sql.Append("CASE");

        if (caseExpression.Operand != null)
        {
            Sql.Append(" ");
            Visit(caseExpression.Operand);
        }

        using (Sql.Indent())
        {
            foreach (var whenClause in caseExpression.WhenClauses)
            {
                Sql.AppendLine().Append("WHEN ");
                Visit(whenClause.Test);
                Sql.Append(" THEN ");
                Visit(whenClause.Result);
            }

            Sql
                .AppendLine()
                .Append("ELSE ");
            if (caseExpression.ElseResult != null)
            {
                Visit(caseExpression.ElseResult);
            }
            else
            {
                Sql.Append("NULL");
            }
        }

        Sql
            .AppendLine()
            .Append("END");

        return caseExpression;
    }
}
