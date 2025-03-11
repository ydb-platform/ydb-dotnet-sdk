using Microsoft.EntityFrameworkCore.Metadata.Conventions;
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

    public override ConventionSet CreateConventionSet()
    {
        var coreConventions = base.CreateConventionSet();
        coreConventions.Add(new YdbStringAttributeConvention(Dependencies));
        return coreConventions;
    }
}
