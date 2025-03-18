using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbTypeMappingPostprocessor(
    QueryTranslationPostprocessorDependencies dependencies,
    RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
    RelationalQueryCompilationContext queryCompilationContext
) : RelationalTypeMappingPostprocessor(dependencies, relationalDependencies, queryCompilationContext);
