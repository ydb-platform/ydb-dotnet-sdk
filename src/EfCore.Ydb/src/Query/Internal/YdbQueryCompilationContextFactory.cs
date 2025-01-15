using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryCompilationContextFactory : IQueryCompilationContextFactory
{
    
    private readonly QueryCompilationContextDependencies _dependencies;
    private readonly RelationalQueryCompilationContextDependencies _relationalDependencies;

    public YdbQueryCompilationContextFactory(
        QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies
    )
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryCompilationContext Create(bool async)
        => new YdbQueryCompilationContext(_dependencies, _relationalDependencies, async);
}
