using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryTranslationPostprocessorFactory(
    QueryTranslationPostprocessorDependencies dependencies,
    RelationalQueryTranslationPostprocessorDependencies relationalDependencies
) : IQueryTranslationPostprocessorFactory
{
    public virtual QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
        => new YdbQueryTranslationPostprocessor(
            dependencies,
            relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext
        );
}
