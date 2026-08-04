using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public partial class YdbQueryableMethodTranslatingExpressionVisitor
{
    protected override bool IsValidSelectExpressionForExecuteDelete(SelectExpression selectExpression)
        => CanGenerateModificationOn(selectExpression);
}
