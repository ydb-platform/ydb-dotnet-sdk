using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using EfCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal;

public class YdbTypeMappingSource : RelationalTypeMappingSource
{
    protected virtual ConcurrentDictionary<string, RelationalTypeMapping[]> StoreTypeMapping { get; }
    protected virtual ConcurrentDictionary<Type, RelationalTypeMapping> ClrTypeMapping { get; }

    #region Mappings

    private readonly YdbBoolTypeMapping _bool = YdbBoolTypeMapping.Default;

    private readonly SByteTypeMapping _int8 = new("INT8", DbType.Byte);
    private readonly ShortTypeMapping _int16 = new("INT16", DbType.Int16);
    private readonly IntTypeMapping _int32 = new("INT32", DbType.Int32);
    private readonly LongTypeMapping _int64 = new("INT64", DbType.Int64);

    private readonly ByteTypeMapping _uint8 = new("UINT8", DbType.SByte);
    private readonly UShortTypeMapping _uint16 = new("UINT16", DbType.UInt16);
    private readonly UIntTypeMapping _uint32 = new("UINT32", DbType.UInt32);
    private readonly ULongTypeMapping _uint64 = new("UINT64", DbType.UInt64);

    private readonly FloatTypeMapping _float = new("FLOAT", DbType.Single);
    private readonly DoubleTypeMapping _double = new("DOUBLE", DbType.Double);

    private readonly StringTypeMapping _string = new("STRING", DbType.String);

    #endregion

    public YdbTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies
    ) : base(dependencies, relationalDependencies)
    {
        var storeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>(StringComparer.OrdinalIgnoreCase)
        {
            { "bool", [_bool] },

            { "int8", [_int8] },
            { "int16", [_int16] },
            { "int32", [_int32] },
            { "int64", [_int64] },

            { "uint8", [_uint8] },
            { "uint16", [_uint16] },
            { "uint32", [_uint32] },
            { "uint64", [_uint64] },

            { "float", [_float] },
            { "double", [_double] },

            { "string", [_string] }
        };
        var clrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
        {
            { typeof(bool), _bool },

            { typeof(sbyte), _int8 },
            { typeof(short), _int16 },
            { typeof(int), _int32 },
            { typeof(long), _int64 },

            { typeof(byte), _uint8 },
            { typeof(ushort), _uint16 },
            { typeof(uint), _uint32 },
            { typeof(ulong), _uint64 },

            { typeof(float), _float },
            { typeof(double), _double },

            { typeof(string), _string }
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
