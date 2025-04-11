using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EntityFrameworkCore.Ydb.Metadata.Internal;

public class YdbAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
    : RelationalAnnotationProvider(dependencies)
{
    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
    {
        if (!designTime)
        {
            yield break;
        }

        // TODO: Add Yson here too?
        if (column is JsonColumn)
        {
            yield break;
        }

        var property = column.PropertyMappings[0].Property;

        if (property.ValueGenerated == ValueGenerated.OnAdd
            && property.ClrType == typeof(int)
            && property.FindTypeMapping()?.Converter == null)
        {
            yield return new Annotation(YdbAnnotationNames.Serial, true);
        }
    }
}
