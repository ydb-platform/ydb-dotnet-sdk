using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using EntityFrameworkCore.Ydb.Query.Expressions.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : QuerySqlGenerator(dependencies)
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
        SkipAliases = true;
        base.VisitDelete(deleteExpression);
        SkipAliases = false;

        return deleteExpression;
    }

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
            Sql.Append(ulong.MaxValue.ToString());
        }

        // ReSharper disable once InvertIf
        if (selectExpression.Offset != null)
        {
            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

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

    protected virtual Expression VisitILike(YdbILikeExpression likeExpression, bool negated = false)
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
