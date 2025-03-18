using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EfCore.Ydb.Metadata.Conventions;

// ReSharper disable once ClassNeverInstantiated.Global
public class YdbConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies
) : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
    public override ConventionSet CreateConventionSet()
    {
        var coreConventions = base.CreateConventionSet();
        coreConventions.Add(new YdbStringAttributeConvention(Dependencies));
        return coreConventions;
    }
}
