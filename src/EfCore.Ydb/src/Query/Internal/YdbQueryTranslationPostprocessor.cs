using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace EfCore.Ydb.Query.Internal;

public class YdbQueryTranslationPostprocessor(
    QueryTranslationPostprocessorDependencies dependencies,
    RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
    RelationalQueryCompilationContext queryCompilationContext
) : RelationalQueryTranslationPostprocessor(dependencies, relationalDependencies, queryCompilationContext)
{
    protected override Expression ProcessTypeMappings(Expression expression) => 
        new YdbTypeMappingPostprocessor(Dependencies, RelationalDependencies, RelationalQueryCompilationContext)
            .Process(expression);
}
