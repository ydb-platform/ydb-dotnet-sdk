using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbTypeMappingPostprocessor : RelationalTypeMappingPostprocessor
{
    public YdbTypeMappingPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext
    ) : base(dependencies, relationalDependencies, queryCompilationContext)
    {
    }
}
