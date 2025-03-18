using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryCompilationContextFactory(
    QueryCompilationContextDependencies dependencies,
    RelationalQueryCompilationContextDependencies relationalDependencies
) : IQueryCompilationContextFactory
{
    public QueryCompilationContext Create(bool async) =>
        new YdbQueryCompilationContext(dependencies, relationalDependencies, async);
}
