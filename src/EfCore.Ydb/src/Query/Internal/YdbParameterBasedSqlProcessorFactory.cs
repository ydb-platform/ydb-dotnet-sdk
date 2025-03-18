using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessorFactory(RelationalParameterBasedSqlProcessorDependencies dependencies)
    : IRelationalParameterBasedSqlProcessorFactory
{
    public RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
        => new YdbParameterBasedSqlProcessor(dependencies, parameters);
}
