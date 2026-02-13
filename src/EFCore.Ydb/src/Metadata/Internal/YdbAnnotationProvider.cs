using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EntityFrameworkCore.Ydb.Metadata.Internal;

public class YdbAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
    : RelationalAnnotationProvider(dependencies)
{
    private static readonly IReadOnlySet<Type> _serialSupportingTypes =
        new HashSet<Type> { typeof(int), typeof(long), typeof(short), typeof(byte) };

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
            && _serialSupportingTypes.Contains(property.ClrType)
            && property.FindTypeMapping()?.Converter == null)
        {
            yield return new Annotation(YdbAnnotationNames.Serial, true);
        }
    }
}
