namespace Ydb.Sdk.Ado.Internal;

using static YdbValueExtensions;

internal class YdbPrimitiveTypeInfo
{
    internal static readonly YdbPrimitiveTypeInfo
        Bool = new(Type.Types.PrimitiveTypeId.Bool, TryPack<bool>(PackBool)),
        Int8 = new(Type.Types.PrimitiveTypeId.Int8, TryPack<sbyte>(PackInt8)),
        Int16 = new(Type.Types.PrimitiveTypeId.Int16, TryPackInt16),
        Int32 = new(Type.Types.PrimitiveTypeId.Int32, TryPackInt32),
        Int64 = new(Type.Types.PrimitiveTypeId.Int64, TryPackInt64),
        Uint8 = new(Type.Types.PrimitiveTypeId.Uint8, TryPack<byte>(PackUint8)),
        Uint16 = new(Type.Types.PrimitiveTypeId.Uint16, TryPackUint16),
        Uint32 = new(Type.Types.PrimitiveTypeId.Uint32, TryPackUint32),
        Uint64 = new(Type.Types.PrimitiveTypeId.Uint64, TryPackUint64),
        Float = new(Type.Types.PrimitiveTypeId.Float, TryPack<float>(PackFloat)),
        Double = new(Type.Types.PrimitiveTypeId.Double, TryPackDouble),
        Bytes = new(Type.Types.PrimitiveTypeId.String, TryPackBytes),
        Text = new(Type.Types.PrimitiveTypeId.Utf8, TryPack<string>(PackText)),
        Json = new(Type.Types.PrimitiveTypeId.Json, TryPack<string>(PackText)),
        JsonDocument = new(Type.Types.PrimitiveTypeId.JsonDocument, TryPack<string>(PackText)),
        Yson = new(Type.Types.PrimitiveTypeId.Yson, TryPackBytes),
        Uuid = new(Type.Types.PrimitiveTypeId.Uuid, TryPack<Guid>(PackUuid)),
        Date = new(Type.Types.PrimitiveTypeId.Date, TryPackDate),
        Date32 = new(Type.Types.PrimitiveTypeId.Date32, TryPackDate32),
        Datetime = new(Type.Types.PrimitiveTypeId.Datetime, TryPack<DateTime>(PackDatetime)),
        Datetime64 = new(Type.Types.PrimitiveTypeId.Datetime64, TryPack<DateTime>(PackDatetime64)),
        Timestamp = new(Type.Types.PrimitiveTypeId.Timestamp, TryPack<DateTime>(PackTimestamp)),
        Timestamp64 = new(Type.Types.PrimitiveTypeId.Timestamp64, TryPack<DateTime>(PackTimestamp64)),
        Interval = new(Type.Types.PrimitiveTypeId.Interval, TryPack<TimeSpan>(PackInterval)),
        Interval64 = new(Type.Types.PrimitiveTypeId.Interval64, TryPack<TimeSpan>(PackInterval64));

    private YdbPrimitiveTypeInfo(Type.Types.PrimitiveTypeId primitiveTypeId, Func<object, Ydb.Value?> pack)
    {
        YdbType = new Type { TypeId = primitiveTypeId };
        NullValue = new TypedValue
            { Type = new Type { OptionalType = new OptionalType { Item = YdbType } }, Value = YdbValueNull };
        Pack = pack;
    }

    internal Type YdbType { get; }
    internal TypedValue NullValue { get; }
    internal Func<object, Ydb.Value?> Pack { get; }

    internal static YdbPrimitiveTypeInfo? TryResolve(System.Type type)
    {
        if (type.IsAssignableFrom(typeof(bool))) return Bool;

        if (type.IsAssignableFrom(typeof(sbyte))) return Int8;
        if (type.IsAssignableFrom(typeof(short))) return Int16;
        if (type.IsAssignableFrom(typeof(int))) return Int32;
        if (type.IsAssignableFrom(typeof(long))) return Int64;

        if (type.IsAssignableFrom(typeof(byte))) return Uint8;
        if (type.IsAssignableFrom(typeof(ushort))) return Uint16;
        if (type.IsAssignableFrom(typeof(uint))) return Uint32;
        if (type.IsAssignableFrom(typeof(ulong))) return Uint64;

        if (type.IsAssignableFrom(typeof(float))) return Float;
        if (type.IsAssignableFrom(typeof(double))) return Double;

        if (type.IsAssignableFrom(typeof(byte[])) || type.IsAssignableFrom(typeof(MemoryStream))) return Bytes;
        if (type.IsAssignableFrom(typeof(string))) return Text;
        if (type.IsAssignableFrom(typeof(Guid))) return Uuid;

        if (type.IsAssignableFrom(typeof(DateOnly))) return Date;
        if (type.IsAssignableFrom(typeof(DateTime))) return Timestamp;
        if (type.IsAssignableFrom(typeof(TimeSpan))) return Interval;

        return null;
    }

    private static Func<object, Ydb.Value?> TryPack<T>(Func<T, Ydb.Value> pack) =>
        value => value is T valueT ? pack(valueT) : null;

    private static Ydb.Value? TryPackInt16(object value) => value switch
    {
        short shortValue => PackInt16(shortValue),
        sbyte sbyteValue => PackInt16(sbyteValue),
        byte byteValue => PackInt16(byteValue),
        _ => null
    };

    private static Ydb.Value? TryPackInt32(object value) => value switch
    {
        int intValue => PackInt32(intValue),
        sbyte sbyteValue => PackInt32(sbyteValue),
        byte byteValue => PackInt32(byteValue),
        short shortValue => PackInt32(shortValue),
        ushort ushortValue => PackInt32(ushortValue),
        _ => null
    };

    private static Ydb.Value? TryPackInt64(object value) => value switch
    {
        long longValue => PackInt64(longValue),
        sbyte sbyteValue => PackInt64(sbyteValue),
        byte byteValue => PackInt64(byteValue),
        short shortValue => PackInt64(shortValue),
        ushort ushortValue => PackInt64(ushortValue),
        int intValue => PackInt64(intValue),
        uint uintValue => PackInt64(uintValue),
        _ => null
    };

    private static Ydb.Value? TryPackUint16(object value) => value switch
    {
        ushort shortValue => PackUint16(shortValue),
        byte byteValue => PackUint16(byteValue),
        _ => null
    };

    private static Ydb.Value? TryPackUint32(object value) => value switch
    {
        uint intValue => PackUint32(intValue),
        byte byteValue => PackUint32(byteValue),
        ushort ushortValue => PackUint32(ushortValue),
        _ => null
    };

    private static Ydb.Value? TryPackUint64(object value) => value switch
    {
        ulong longValue => PackUint64(longValue),
        byte byteValue => PackUint64(byteValue),
        ushort ushortValue => PackUint64(ushortValue),
        uint uintValue => PackUint64(uintValue),
        _ => null
    };

    private static Ydb.Value? TryPackDouble(object value) => value switch
    {
        double doubleValue => PackDouble(doubleValue),
        float floatValue => PackDouble(floatValue),
        _ => null
    };

    private static Ydb.Value? TryPackBytes(object value) => value switch
    {
        byte[] bytesValue => PackBytes(bytesValue),
        MemoryStream memoryStream => PackBytes(memoryStream.ToArray()),
        _ => null
    };

    private static Ydb.Value? TryPackDate(object value) => value switch
    {
        DateTime dateTimeValue => PackDate(dateTimeValue),
        DateOnly dateOnlyValue => PackDate(dateOnlyValue.ToDateTime(TimeOnly.MinValue)),
        _ => null
    };

    private static Ydb.Value? TryPackDate32(object value) => value switch
    {
        DateTime dateTimeValue => PackDate32(dateTimeValue),
        DateOnly dateOnlyValue => PackDate32(dateOnlyValue.ToDateTime(TimeOnly.MinValue)),
        _ => null
    };
}
