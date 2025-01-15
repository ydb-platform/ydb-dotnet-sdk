using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal;

public class YdbTypeMappingSource : RelationalTypeMappingSource
{
    protected virtual ConcurrentDictionary<string, RelationalTypeMapping[]> StoreTypeMapping { get; }
    protected virtual ConcurrentDictionary<Type, RelationalTypeMapping> ClrTypeMapping { get; }

    #region Mappings

    private readonly IntTypeMapping _int32 = new("INT32", DbType.Int32);

    #endregion

    public YdbTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies
    ) : base(dependencies, relationalDependencies)
    {
        var storeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>(StringComparer.OrdinalIgnoreCase)
        {
            { "integer", [_int32] }
        };
        var clrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
        {
            { typeof(int), _int32 }
        };

        StoreTypeMapping = new ConcurrentDictionary<string, RelationalTypeMapping[]>(storeTypeMappings);
        ClrTypeMapping = new ConcurrentDictionary<Type, RelationalTypeMapping>(clrTypeMappings);
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        => base.FindMapping(mappingInfo)
           ?? FindBaseMapping(mappingInfo);

    protected virtual RelationalTypeMapping? FindBaseMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;
        var storeTypeNameBase = mappingInfo.StoreTypeNameBase;

        if (storeTypeName is not null)
        {
            if (StoreTypeMapping.TryGetValue(storeTypeName, out var mappings))
            {
                foreach (var m in mappings)
                {
                    if (m.ClrType == clrType)
                    {
                        return m;
                    }
                }
            }
        }

        if (clrType is not null)
        {
            if (ClrTypeMapping.TryGetValue(clrType, out var mapping))
            {
                return mapping;
            }
        }

        return null;
    }
}
