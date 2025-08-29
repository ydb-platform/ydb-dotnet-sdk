using System.Globalization;
using System.Numerics;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Ydb.Sdk.Ado.Internal;

internal static class YdbTypedValueExtensions
{
    internal static TypedValue Null(this Type.Types.PrimitiveTypeId primitiveTypeId) => new()
    {
        Type = new Type { OptionalType = new OptionalType { Item = new Type { TypeId = primitiveTypeId } } },
        Value = new Ydb.Value { NullFlagValue = NullValue.NullValue }
    };

    internal static string ToYql(this TypedValue typedValue) => ToYql(typedValue.Type);

    private static string ToYql(Type type) =>
        type.TypeCase switch
        {
            Type.TypeOneofCase.TypeId => type.TypeId.ToString(),
            Type.TypeOneofCase.DecimalType => $"Decimal({type.DecimalType.Precision}, {type.DecimalType.Scale})",
            Type.TypeOneofCase.OptionalType => $"{ToYql(type.OptionalType.Item)}?",
            Type.TypeOneofCase.ListType => $"List<{ToYql(type.ListType.Item)}>",
            Type.TypeOneofCase.VoidType => "Void",
            _ => "Unknown"
        };

    internal static TypedValue NullDecimal(byte precision, byte scale) => new()
    {
        Type = new Type
        {
            OptionalType = new OptionalType
            {
                Item = new Type { DecimalType = new DecimalType { Precision = precision, Scale = scale } }
            }
        },
        Value = new Ydb.Value { NullFlagValue = NullValue.NullValue }
    };

    internal static TypedValue Text(this string value) => MakeText(Type.Types.PrimitiveTypeId.Utf8, value);

    internal static TypedValue Bool(this bool value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Bool, new Ydb.Value { BoolValue = value });

    internal static TypedValue Int8(this sbyte value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Int8, new Ydb.Value { Int32Value = value });

    internal static TypedValue Int16(this short value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Int16, new Ydb.Value { Int32Value = value });

    internal static TypedValue Int32(this int value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Int32, new Ydb.Value { Int32Value = value });

    internal static TypedValue Int64(this long value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Int64, new Ydb.Value { Int64Value = value });

    internal static TypedValue Uint8(this byte value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Uint8, new Ydb.Value { Uint32Value = value });

    internal static TypedValue Uint16(this ushort value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Uint16, new Ydb.Value { Uint32Value = value });

    internal static TypedValue Uint32(this uint value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Uint32, new Ydb.Value { Uint32Value = value });

    internal static TypedValue Uint64(this ulong value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Uint64, new Ydb.Value { Uint64Value = value });

    internal static TypedValue Float(this float value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Float, new Ydb.Value { FloatValue = value });

    internal static TypedValue Double(this double value) =>
        MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Double, new Ydb.Value { DoubleValue = value });

    internal static TypedValue Decimal(this decimal value, byte precision, byte scale)
    {
        var bits = decimal.GetBits(value);
        var scale0 = (bits[3] >> 16) & 0xFF;
        var isNegative = (bits[3] & unchecked((int)0x80000000)) != 0;

        if (scale0 > scale)
            throw new OverflowException(
                $"Decimal scale overflow: fractional digits {scale0} exceed allowed {scale} for DECIMAL({precision},{scale}). Value={value}");

        var lo = (uint)bits[0];
        var mid = (uint)bits[1];
        var hi = (uint)bits[2];
        var mantissa = ((BigInteger)hi << 64) | ((BigInteger)mid << 32) | lo;

        var delta = scale - scale0;
        if (delta > 0)
            mantissa *= BigInteger.Pow(10, delta);

        var totalDigits = mantissa.IsZero ? 1 : mantissa.ToString(CultureInfo.InvariantCulture).Length;
        if (totalDigits > precision)
            throw new OverflowException(
                $"Decimal precision overflow: total digits {totalDigits} exceed allowed {precision} for DECIMAL({precision},{scale}). Value={value}");

        if (isNegative) mantissa = -mantissa;

        var mod128 = (mantissa % (BigInteger.One << 128) + (BigInteger.One << 128)) % (BigInteger.One << 128);
        var low = (ulong)(mod128 & ((BigInteger.One << 64) - 1));
        var high = (ulong)(mod128 >> 64);

        return new TypedValue
        {
            Type = new Type { DecimalType = new DecimalType { Precision = precision, Scale = scale } },
            Value = new Ydb.Value { Low128 = low, High128 = high }
        };
    }

    internal static TypedValue Bytes(this byte[] value) => MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.String,
        new Ydb.Value { BytesValue = ByteString.CopyFrom(value) });

    internal static TypedValue Json(this string value) => MakeText(Type.Types.PrimitiveTypeId.Json, value);

    internal static TypedValue JsonDocument(this string value) =>
        MakeText(Type.Types.PrimitiveTypeId.JsonDocument, value);

    internal static TypedValue Uuid(this Guid value)
    {
        var bytes = value.ToByteArray();
        var low = BitConverter.ToUInt64(bytes, 0);
        var high = BitConverter.ToUInt64(bytes, 8);

        return MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Uuid, new Ydb.Value { Low128 = low, High128 = high });
    }

    internal static TypedValue Date(this DateTime value) => MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Date,
        new Ydb.Value { Uint32Value = (uint)(value.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerDay) });

    internal static TypedValue Date32(this DateTime value) => MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId.Date32,
        new Ydb.Value { Int32Value = (int)(value.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerDay) });

    internal static TypedValue Datetime(this DateTime dateTimeValue) => MakePrimitiveTypedValue(
        Type.Types.PrimitiveTypeId.Datetime, new Ydb.Value
            { Uint32Value = (uint)(dateTimeValue.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond) }
    );

    internal static TypedValue Datetime64(this DateTime dateTimeValue) => MakePrimitiveTypedValue(
        Type.Types.PrimitiveTypeId.Datetime64,
        new Ydb.Value { Int64Value = dateTimeValue.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond }
    );

    internal static TypedValue Timestamp(this DateTime dateTimeValue) => MakePrimitiveTypedValue(
        Type.Types.PrimitiveTypeId.Timestamp, new Ydb.Value
        {
            Uint64Value = (ulong)(dateTimeValue.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpanUtils.TicksPerMicrosecond
        }
    );

    internal static TypedValue Timestamp64(this DateTime dateTimeValue) => MakePrimitiveTypedValue(
        Type.Types.PrimitiveTypeId.Timestamp64, new Ydb.Value
            { Int64Value = (dateTimeValue.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpanUtils.TicksPerMicrosecond }
    );

    internal static TypedValue Interval(this TimeSpan timeSpanValue) => MakePrimitiveTypedValue(
        Type.Types.PrimitiveTypeId.Interval,
        new Ydb.Value { Int64Value = timeSpanValue.Ticks / TimeSpanUtils.TicksPerMicrosecond }
    );

    internal static TypedValue Interval64(this TimeSpan timeSpanValue) => MakePrimitiveTypedValue(
        Type.Types.PrimitiveTypeId.Interval64,
        new Ydb.Value { Int64Value = timeSpanValue.Ticks / TimeSpanUtils.TicksPerMicrosecond }
    );

    internal static TypedValue List(this IReadOnlyList<TypedValue> values)
    {
        if (values.Count == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(values));
        }

        var value = new Ydb.Value();
        value.Items.Add(values.Select(v => v.Value));

        return new TypedValue
        {
            Type = new Type { ListType = new ListType { Item = values[0].Type } },
            Value = value
        };
    }

    private static TypedValue MakeText(Type.Types.PrimitiveTypeId primitiveTypeId, string textValue) =>
        MakePrimitiveTypedValue(primitiveTypeId, new Ydb.Value { TextValue = textValue });

    private static TypedValue MakePrimitiveTypedValue(Type.Types.PrimitiveTypeId primitiveTypeId, Ydb.Value value) =>
        new() { Type = new Type { TypeId = primitiveTypeId }, Value = value };
}
