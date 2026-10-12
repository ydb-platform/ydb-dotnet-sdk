using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbEvaluatableExpressionFilter(
    EvaluatableExpressionFilterDependencies dependencies,
    RelationalEvaluatableExpressionFilterDependencies relationalDependencies)
    : RelationalEvaluatableExpressionFilter(dependencies, relationalDependencies)
{
    public override bool IsEvaluatableExpression(Expression expression, IModel model)
    {
        if (expression is MethodCallExpression methodCall
            && methodCall.Method.DeclaringType == typeof(YdbFunctionExtension))
        {
            return false;
        }

        return base.IsEvaluatableExpression(expression, model);
    }
}
