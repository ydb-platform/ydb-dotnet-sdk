using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EfCore.Ydb.Metadata.Conventions;

public class YdbConventionSetBuilder
    : RelationalConventionSetBuilder
{
    public YdbConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies
    ) : base(dependencies, relationalDependencies)
    {
    }
}
