using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.Query.Internal;

public class YdbQueryCompilationContext(
    QueryCompilationContextDependencies dependencies,
    RelationalQueryCompilationContextDependencies relationalDependencies,
    bool async
) : RelationalQueryCompilationContext(dependencies, relationalDependencies, async);
