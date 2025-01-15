using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessor
    : RelationalParameterBasedSqlProcessor
{
    public YdbParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters
    ) : base(dependencies, parameters)
    {
    }
}
