using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EntityFrameworkCore.Ydb.Metadata.Internal;

public class YdbAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
    : RelationalAnnotationProvider(dependencies)
{
    private static readonly HashSet<Type> SerialSupportingTypes =
        [typeof(int), typeof(long), typeof(short)];

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

        if (column.PropertyMappings.Any(mapping =>
                mapping.Property.ValueGenerated == ValueGenerated.OnAdd
                && SerialSupportingTypes.Contains(mapping.Property.ClrType)
                && mapping.Property.FindAnnotation(RelationalAnnotationNames.DefaultValue) == null
                && mapping.Property.FindAnnotation(RelationalAnnotationNames.DefaultValueSql) == null
                && mapping.Property.FindTypeMapping()?.Converter == null))
        {
            yield return new Annotation(YdbAnnotationNames.Serial, true);
        }
    }
}
