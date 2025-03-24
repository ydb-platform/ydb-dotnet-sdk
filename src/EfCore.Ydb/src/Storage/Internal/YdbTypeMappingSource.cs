using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Numerics;
using System.Text.Json;
using EfCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Type = System.Type;

namespace EfCore.Ydb.Storage.Internal;

public sealed class YdbTypeMappingSource : RelationalTypeMappingSource
{
    private ConcurrentDictionary<string, RelationalTypeMapping[]> StoreTypeMapping { get; }
    private ConcurrentDictionary<Type, RelationalTypeMapping> ClrTypeMapping { get; }

    #region Mappings

    private readonly YdbBoolTypeMapping _bool = YdbBoolTypeMapping.Default;

    private readonly SByteTypeMapping _int8 = new("Int8", DbType.SByte);
    private readonly ShortTypeMapping _int16 = new("Int16", DbType.Int16);
    private readonly IntTypeMapping _int32 = new("Int32", DbType.Int32);
    private readonly LongTypeMapping _int64 = new("Int64", DbType.Int64);

    private readonly ByteTypeMapping _uint8 = new("Uint8", DbType.Byte);
    private readonly UShortTypeMapping _uint16 = new("Uint16", DbType.UInt16);
    private readonly UIntTypeMapping _uint32 = new("Uint32", DbType.UInt32);
    private readonly ULongTypeMapping _uint64 = new("Uint64", DbType.UInt64);

    private readonly FloatTypeMapping _float = new("Float", DbType.Single);
    private readonly DoubleTypeMapping _double = new("Double", DbType.Double);
    private readonly YdbDecimalTypeMapping _biginteger = new(typeof(BigInteger));
    private readonly YdbDecimalTypeMapping _decimal = new(typeof(decimal));
    private readonly YdbDecimalTypeMapping _decimalAsDouble = new(typeof(double));
    private readonly YdbDecimalTypeMapping _decimalAsFloat = new(typeof(float));

    private readonly StringTypeMapping _text = new("Text", DbType.String);
    private readonly YdbStringTypeMapping _ydbString = YdbStringTypeMapping.Default;
    private readonly YdbBytesTypeMapping _bytes = YdbBytesTypeMapping.Default;
    private readonly YdbJsonTypeMapping _json = new("Json", typeof(JsonElement), DbType.String);

    private readonly DateOnlyTypeMapping _date = new("Date");
    private readonly DateTimeTypeMapping _dateTime = new("Datetime");
    private readonly DateTimeTypeMapping _timestamp = new("Timestamp");
    private readonly TimeSpanTypeMapping _interval = new("Interval");

    #endregion

    public YdbTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies
    ) : base(dependencies, relationalDependencies)
    {
        var storeTypeMappings = new Dictionary<string, RelationalTypeMapping[]>(StringComparer.OrdinalIgnoreCase)
        {
            { "Bool", [_bool] },

            { "Int8", [_int8] },
            { "Int16", [_int16] },
            { "Int32", [_int32] },
            { "Int64", [_int64] },

            { "Uint8", [_uint8] },
            { "Uint16", [_uint16] },
            { "Uint32", [_uint32] },
            { "Uint64", [_uint64] },

            { "Float", [_float] },
            { "Double", [_double] },

            { "Decimal", [_decimal, _decimalAsDouble, _decimalAsFloat, _biginteger] },

            { "Text", [_text] },
            { "String", [_ydbString] },
            { "Json", [_json] },

            { "Bytes", [_bytes] },

            { "Date", [_date] },
            { "DateTime", [_dateTime] },
            { "Timestamp", [_timestamp] },
            { "Interval", [_interval] }
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
            { typeof(decimal), _decimal },

            { typeof(string), _text },
            { typeof(byte[]), _bytes },
            { typeof(JsonElement), _json },

            { typeof(DateOnly), _date },
            { typeof(DateTime), _timestamp },
            { typeof(TimeSpan), _interval }
        };

        StoreTypeMapping = new ConcurrentDictionary<string, RelationalTypeMapping[]>(storeTypeMappings);
        ClrTypeMapping = new ConcurrentDictionary<Type, RelationalTypeMapping>(clrTypeMappings);
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        => FindBaseMapping(mappingInfo)
           ?? base.FindMapping(mappingInfo);

    private RelationalTypeMapping? FindBaseMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;

        // Special case.
        // If property has [YdbString] attribute then we use STRING type instead of TEXT
        if (mappingInfo.StoreTypeName == "string")
        {
            return _ydbString;
        }

        if (storeTypeName is null)
        {
            return clrType is null ? null : ClrTypeMapping.GetValueOrDefault(clrType);
        }

        if (!StoreTypeMapping.TryGetValue(storeTypeName, out var mappings))
        {
            return clrType is null ? null : ClrTypeMapping.GetValueOrDefault(clrType);
        }

        foreach (var m in mappings)
        {
            if (m.ClrType == clrType)
            {
                return m;
            }
        }

        return clrType is null ? null : ClrTypeMapping.GetValueOrDefault(clrType);
    }
}
