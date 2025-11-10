using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EntityFrameworkCore.Ydb.Metadata.Conventions;

public class YdbValueGenerationConvention(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalValueGenerationConvention(dependencies, relationalDependencies)
{
    protected override ValueGenerated? GetValueGenerated(IConventionProperty property)
    {
        var table = property.GetMappedStoreObjects(StoreObjectType.Table).FirstOrDefault();

        return !MappingStrategyAllowsValueGeneration(property, property.DeclaringType.GetMappingStrategy())
            ? null
            : table.Name != null
                ? GetValueGenerated(property, table)
                : property.DeclaringType.IsMappedToJson()
#pragma warning disable EF1001 // Internal EF Core API usage.
                  && property.IsOrdinalKeyProperty()
#pragma warning restore EF1001 // Internal EF Core API usage.
                  && (property.DeclaringType as IReadOnlyEntityType)?.FindOwnership()!.IsUnique == false
                    ? ValueGenerated.OnAddOrUpdate
                    : property.GetMappedStoreObjects(StoreObjectType.InsertStoredProcedure).Any()
                        ? GetValueGenerated((IReadOnlyProperty)property)
                        : null;
    }

    private new static ValueGenerated? GetValueGenerated(
        IReadOnlyProperty property,
        in StoreObjectIdentifier storeObject
    )
    {
        var valueGenerated = GetValueGenerated(property);
        return valueGenerated
               ?? (property.GetComputedColumnSql(storeObject) != null
                   ? ValueGenerated.OnAddOrUpdate
                   : property.GetDefaultValueSql(storeObject) != null
                       ? ValueGenerated.OnAdd
                       : null);
    }
}
