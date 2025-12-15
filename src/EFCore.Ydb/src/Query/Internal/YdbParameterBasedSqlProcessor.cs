using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
    RelationalParameterBasedSqlProcessorParameters parameters
) : RelationalParameterBasedSqlProcessor(dependencies, parameters)
{
#if EFCORE9
    protected override Expression ProcessSqlNullability(Expression queryExpression,
        IReadOnlyDictionary<string, object?> parametersValues, out bool canCache)
        => new YdbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersValues,
            out canCache);
#else
    protected override Expression ProcessSqlNullability(Expression queryExpression, 
        ParametersCacheDecorator parametersDecorator)
        => new YdbSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersDecorator);
#endif
}
