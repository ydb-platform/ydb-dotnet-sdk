using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public partial class YdbQueryableMethodTranslatingExpressionVisitor
{
    protected override bool IsValidSelectExpressionForExecuteDelete(
        SelectExpression selectExpression,
        StructuralTypeShaperExpression shaper,
        [NotNullWhen(true)] out TableExpression? tableExpression)
    {
        if (!CanGenerateModificationOn(selectExpression))
        {
            tableExpression = null;
            return false;
        }

        var projectionBindingExpression = (ProjectionBindingExpression)shaper.ValueBufferExpression;
        var entityProjectionExpression =
            (StructuralTypeProjectionExpression)selectExpression.GetProjection(projectionBindingExpression);
        var column = entityProjectionExpression.BindProperty(shaper.StructuralType.GetProperties().First());

        tableExpression = selectExpression.Tables
            .Select(table => table.UnwrapJoin())
            .OfType<TableExpression>()
            .SingleOrDefault(table => table.Alias == column.TableAlias);

        return tableExpression is not null;
    }
}
