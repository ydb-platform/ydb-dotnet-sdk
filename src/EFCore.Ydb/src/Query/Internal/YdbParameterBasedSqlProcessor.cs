using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters
) : RelationalParameterBasedSqlProcessor(dependencies, parameters)
{
    protected override Expression ProcessSqlNullability(Expression queryExpression, IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        =>new YdbParameterBasedSqlProcessor(Dependencies, Parameters).ProcessSqlNullability(queryExpression, parametersValues, out canCache);
}
