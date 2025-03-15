using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryCompilationContext : RelationalQueryCompilationContext
{
    public YdbQueryCompilationContext(
        QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies,
        bool async
    ) : base(dependencies, relationalDependencies, async)
    {
    }
}
