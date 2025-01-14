using Microsoft.EntityFrameworkCore.Query;

namespace Ef.Ydb.Query.Internal;

public class YdbParameterBasedSqlProcessorFactory
    : IRelationalParameterBasedSqlProcessorFactory
{
    private readonly RelationalParameterBasedSqlProcessorDependencies _dependencies;

    public YdbParameterBasedSqlProcessorFactory(
        RelationalParameterBasedSqlProcessorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public RelationalParameterBasedSqlProcessor Create(RelationalParameterBasedSqlProcessorParameters parameters)
        => new YdbParameterBasedSqlProcessor(_dependencies, parameters);
}
