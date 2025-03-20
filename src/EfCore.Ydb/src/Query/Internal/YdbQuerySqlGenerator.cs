using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Query.Internal;

public class YdbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : QuerySqlGenerator(dependencies)
{
    private bool SkipAliases { get; set; }
    private ISqlGenerationHelper SqlGenerationHelper => Dependencies.SqlGenerationHelper;

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
            // I'm not sure if this always work.
            // But for now I didn't find test where it fails
            GenerateOnSubquery(null, select);
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
            // I'm not sure if this always work.
            // But for now I didn't find test where it fails
            GenerateOnSubquery(updateExpression.ColumnValueSetters, select);
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
        IReadOnlyList<ColumnValueSetter>? columnValueSetters,
        SelectExpression select
    )
    {
        Sql.Append(" ON ").AppendLine().Append("SELECT ");

        if (columnValueSetters == null)
        {
            Sql.Append(" * ");
        }
        else
        {
            var columnName = columnValueSetters[0].Column.Name;
            Visit(columnValueSetters[0].Value);
            Sql.Append(" AS ").Append(columnName);

            foreach (var columnValueSetter in columnValueSetters.Skip(1))
            {
                Sql.Append(", ");
                Visit(columnValueSetter.Value);
                Sql.Append(" AS ").Append(columnValueSetter.Column.Name);
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

    /// <inheritdoc />    
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
            Sql.Append(ulong.MaxValue.ToString());
        }

        // ReSharper disable once InvertIf
        if (selectExpression.Offset != null)
        {
            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
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

    protected override string GetOperator(SqlBinaryExpression binaryExpression)
        => binaryExpression.OperatorType == ExpressionType.Add
           && binaryExpression.Type == typeof(string)
            ? " || "
            : base.GetOperator(binaryExpression);

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
                    Sql
                        .Append(isFirst ? "" : ".")
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
}
