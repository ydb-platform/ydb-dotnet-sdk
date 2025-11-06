using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado.YdbType;
using Type = System.Type;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public sealed class YdbTypeMappingSource(
    TypeMappingSourceDependencies dependencies,
    RelationalTypeMappingSourceDependencies relationalDependencies
) : RelationalTypeMappingSource(dependencies, relationalDependencies)
{
    private static readonly ConcurrentDictionary<RelationalTypeMappingInfo, RelationalTypeMapping> DecimalCache = new();

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

        { typeof(Guid), Guid },

        { typeof(string), Text },
        { typeof(byte[]), Bytes },
        { typeof(JsonElement), Json },

        { typeof(DateOnly), Date },
        { typeof(DateTime), Timestamp },
        { typeof(TimeSpan), Interval }
    };

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        if (mappingInfo.ClrType == typeof(decimal))
        {
            return DecimalCache.GetOrAdd(mappingInfo, static mi => Decimal.Clone(mi));
        }

        return base.FindMapping(mappingInfo) ?? FindBaseMapping(mappingInfo)?.Clone(mappingInfo);
    }

    private static RelationalTypeMapping? FindBaseMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        var storeTypeName = mappingInfo.StoreTypeName;

        if (storeTypeName is not null && StoreTypeMapping.TryGetValue(storeTypeName, out var mappings))
        {
            // We found the user-specified store type. No CLR type was provided - we're probably
            // scaffolding from an existing database, take the first mapping as the default.
            if (clrType is null)
            {
                return mappings[0];
            }

            // A CLR type was provided - look for a mapping between the store and CLR types. If not found, fail
            // immediately.
            foreach (var m in mappings)
            {
                if (m.ClrType == clrType)
                {
                    return m;
                }
            }
        }

        return clrType is null ? null : ClrTypeMapping.GetValueOrDefault(clrType);
    }

    protected override RelationalTypeMapping? FindCollectionMapping(
        RelationalTypeMappingInfo info,
        Type modelType,
        Type? providerType,
        CoreTypeMapping? elementMapping
    )
    {
        var elementType = modelType.IsArray
            ? modelType.GetElementType()
            : modelType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IList<>))?
                .GetGenericArguments()[0];

        if (elementType == null)
            return null;

        elementType = Nullable.GetUnderlyingType(elementType) ?? elementType;

        var typeMapping = ClrTypeMapping.GetValueOrDefault(elementType);

        if (typeMapping == null)
            return null;

        var ydbDbType = typeMapping is IYdbTypeMapping ydbTypeMapping
            ? ydbTypeMapping.YdbDbType
            : (typeMapping.DbType ?? DbType.Object).ToYdbDbType();
        
        return new YdbListTypeMapping(ydbDbType, )
    }
}
