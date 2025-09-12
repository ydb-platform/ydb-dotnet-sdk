using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Type = System.Type;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public sealed class YdbTypeMappingSource(
    TypeMappingSourceDependencies dependencies,
    RelationalTypeMappingSourceDependencies relationalDependencies
) : RelationalTypeMappingSource(dependencies, relationalDependencies)
{
    #region Mappings

    private static readonly YdbBoolTypeMapping Bool = YdbBoolTypeMapping.Default;

    private static readonly SByteTypeMapping Int8 = new("Int8", DbType.SByte);
    private static readonly ShortTypeMapping Int16 = new("Int16", DbType.Int16);
    private static readonly IntTypeMapping Int32 = new("Int32", DbType.Int32);
    private static readonly LongTypeMapping Int64 = new("Int64", DbType.Int64);

    private static readonly ByteTypeMapping Uint8 = new("Uint8", DbType.Byte);
    private static readonly UShortTypeMapping Uint16 = new("Uint16", DbType.UInt16);
    private static readonly UIntTypeMapping Uint32 = new("Uint32", DbType.UInt32);
    private static readonly ULongTypeMapping Uint64 = new("Uint64", DbType.UInt64);

    private static readonly FloatTypeMapping Float = new("Float", DbType.Single);
    private static readonly DoubleTypeMapping Double = new("Double", DbType.Double);

    private static readonly YdbDecimalTypeMapping Decimal = YdbDecimalTypeMapping.Default;

    private static readonly GuidTypeMapping Guid = YdbGuidTypeMapping.Default;

    private static readonly YdbTextTypeMapping Text = YdbTextTypeMapping.Default;
    private static readonly YdbBytesTypeMapping Bytes = YdbBytesTypeMapping.Default;
    private static readonly YdbJsonTypeMapping Json = new("Json", typeof(JsonElement), null);

    private static readonly YdbDateOnlyTypeMapping Date = new("Date");
    private static readonly DateTimeTypeMapping DateTime = new("DateTime");

    private static readonly YdbDateTimeTypeMapping Timestamp = new("Timestamp", DbType.DateTime);

    // TODO: Await interval in Ydb.Sdk
    private static readonly TimeSpanTypeMapping Interval = new("Interval", DbType.Object);

    #endregion

    private static readonly Dictionary<string, RelationalTypeMapping[]> StoreTypeMapping =
        new(StringComparer.OrdinalIgnoreCase)
        {
            { "Bool", [Bool] },

            { "Int8", [Int8] },
            { "Int16", [Int16] },
            { "Int32", [Int32] },
            { "Int64", [Int64] },

            { "Uint8", [Uint8] },
            { "Uint16", [Uint16] },
            { "Uint32", [Uint32] },
            { "Uint64", [Uint64] },

            { "Float", [Float] },
            { "Double", [Double] },

            { "Decimal", [Decimal] },

            { "Guid", [Guid] },

            { "Date", [Date] },
            { "DateTime", [DateTime] },
            { "Timestamp", [Timestamp] },
            { "Interval", [Interval] },

            { "Text", [Text] },
            { "Bytes", [Bytes] },

            { "Json", [Json] }
        };

    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMapping = new()
    {
        { typeof(bool), Bool },

        { typeof(sbyte), Int8 },
        { typeof(short), Int16 },
        { typeof(int), Int32 },
        { typeof(long), Int64 },

        { typeof(byte), Uint8 },
        { typeof(ushort), Uint16 },
        { typeof(uint), Uint32 },
        { typeof(ulong), Uint64 },

        { typeof(float), Float },
        { typeof(double), Double },
        { typeof(decimal), Decimal },

        { typeof(Guid), Guid },

        { typeof(string), Text },
        { typeof(byte[]), Bytes },
        { typeof(JsonElement), Json },

        { typeof(DateOnly), Date },
        { typeof(DateTime), Timestamp },
        { typeof(TimeSpan), Interval }
    };

    private readonly ConcurrentDictionary<RelationalTypeMappingInfo, RelationalTypeMapping> _decimalCache = new();

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (mappingInfo.ClrType == typeof(decimal))
        {
            return _decimalCache.GetOrAdd(mappingInfo, static mi => Decimal.Clone(mi));
        }

        return base.FindMapping(mappingInfo) ?? FindBaseMapping(mappingInfo)?.Clone(mappingInfo);
    }

    private static RelationalTypeMapping? FindBaseMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;

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
