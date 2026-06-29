using System;
using System.Collections.Generic;
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

        var complexSelect = IsComplexSelect(deleteExpression.SelectExpression, deleteExpression.Table);

        if (select.Predicate is InExpression predicate)
        {
            Sql.Append(" ON ");
            Visit(predicate.Subquery);
        }
        else if (!complexSelect)
        {
            GenerateSimpleWhere(select, skipAliases: true);
        }
        else
        {
            GenerateOnSubquery(deleteExpression.Table, null, select);
        }

        return deleteExpression;
    }

    protected override Expression VisitUpdate(UpdateExpression updateExpression)
    {
        Sql.Append("UPDATE ");

        SkipAliases = true;
        Visit(updateExpression.Table);
        SkipAliases = false;

        var select = updateExpression.SelectExpression;

        var complexSelect = IsComplexSelect(updateExpression.SelectExpression, updateExpression.Table);

        if (!complexSelect)
        {
            GenerateUpdateColumnSetters(updateExpression);
            GenerateSimpleWhere(select, skipAliases: true);
        }
        else
        {
            GenerateOnSubquery(updateExpression.Table, updateExpression.ColumnValueSetters, select);
        }

        return updateExpression;
    }

    private void GenerateSimpleWhere(SelectExpression select, bool skipAliases)
    {
        var predicate = select.Predicate;
        if (predicate == null) return;

        Sql.AppendLine().Append("WHERE ");
        if (skipAliases) SkipAliases = true;
        Visit(predicate);
        if (skipAliases) SkipAliases = false;
    }

    private void GenerateUpdateColumnSetters(UpdateExpression updateExpression)
    {
        Sql.AppendLine()
            .Append("SET ")
            .Append(SqlGenerationHelper.DelimitIdentifier(updateExpression.ColumnValueSetters[0].Column.Name))
            .Append(" = ");

        SkipAliases = true;
        Visit(updateExpression.ColumnValueSetters[0].Value);
        SkipAliases = false;

        using (Sql.Indent())
        {
            foreach (var columnValueSetter in updateExpression.ColumnValueSetters.Skip(1))
            {
                Sql.AppendLine(",")
                    .Append(SqlGenerationHelper.DelimitIdentifier(columnValueSetter.Column.Name))
                    .Append(" = ");
                SkipAliases = true;
                Visit(columnValueSetter.Value);
                SkipAliases = false;
            }
        }
    }

    private void GenerateOnSubquery(
        TableExpression? updateTable,
        IReadOnlyList<ColumnValueSetter>? columnValueSetters,
        SelectExpression select
    )
    {
        Sql.Append(" ON ").AppendLine().Append("SELECT ");

        var first = true;
        foreach (var keyColumn in FindKeyColumnsForUpdateOn(updateTable!))
        {
            if (!first)
            {
                Sql.Append(", ");
            }

            Visit(keyColumn);
            AppendSelectAlias(keyColumn.Name);
            first = false;
        }

        if (columnValueSetters != null)
        {
            foreach (var columnValueSetter in columnValueSetters)
            {
                if (!first)
                {
                    Sql.Append(", ");
                }

                SkipAliases = true;
                Visit(columnValueSetter.Value);
                SkipAliases = false;
                AppendSelectAlias(columnValueSetter.Column.Name);
                first = false;
            }
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

    private static ImmutableList<ColumnExpression> FindKeyColumnsForUpdateOn(TableExpression updateTable)
    {
        var updateTableAlias = updateTable.Alias;

        var entityType = updateTable.Table.EntityTypeMappings
            .Select(m => m.TypeBase)
            .OfType<IEntityType>()
            .FirstOrDefault();

        if (entityType?.FindPrimaryKey() is not { } primaryKey)
        {
            throw new InvalidOperationException(
                $"Could not determine key columns for UPDATE ON over `{updateTable.Name}`.");
        }

        var storeObject = StoreObjectIdentifier.Table(updateTable.Name, updateTable.Schema);

        return primaryKey.Properties
            .Select(property =>
            {
                var columnName = property.GetColumnName(storeObject)
                    ?? throw new InvalidOperationException(
                        $"Could not determine key column name for `{property.Name}` on `{updateTable.Name}`.");

                return new ColumnExpression(
                    columnName,
                    updateTableAlias,
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
        || select.Predicate is InExpression
        || !(select.Tables.Count == 1 && select.Tables[0].Equals(fromTable));

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
