using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryCompilationContext(
    QueryCompilationContextDependencies dependencies,
    RelationalQueryCompilationContextDependencies relationalDependencies,
    bool async
) : RelationalQueryCompilationContext(dependencies, relationalDependencies, async);
