using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Ydb.Sdk.Ado.Internal;

internal static class YdbValueExtensions
{
    private const byte MaxPrecisionDecimal = 29;

    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly decimal[] Pow10 = CreatePow10();

    private static decimal[] CreatePow10()
    {
        var a = new decimal[29];
        a[0] = 1m;
        for (var i = 1; i < a.Length; i++) a[i] = a[i - 1] * 10m; // 1..1e28
        return a;
    }

    internal static readonly Ydb.Value YdbValueNull = new() { NullFlagValue = NullValue.NullValue };

    internal static bool IsNull(this Ydb.Value value) => value.ValueCase == Ydb.Value.ValueOneofCase.NullFlagValue;

    internal static Ydb.Value PackBool(bool value) => new() { BoolValue = value };
    internal static bool UnpackBool(this Ydb.Value value) => value.BoolValue;

    internal static Ydb.Value PackInt8(sbyte value) => new() { Int32Value = value };
    internal static sbyte UnpackInt8(this Ydb.Value value) => (sbyte)value.Int32Value;

    internal static Ydb.Value PackInt16(short value) => new() { Int32Value = value };
    internal static short UnpackInt16(this Ydb.Value value) => (short)value.Int32Value;

    internal static Ydb.Value PackInt32(int value) => new() { Int32Value = value };
    internal static int UnpackInt32(this Ydb.Value value) => value.Int32Value;

    internal static Ydb.Value PackInt64(long value) => new() { Int64Value = value };
    internal static long UnpackInt64(this Ydb.Value value) => value.Int64Value;

    internal static Ydb.Value PackUint8(byte value) => new() { Uint32Value = value };
    internal static byte UnpackUint8(this Ydb.Value value) => (byte)value.Uint32Value;

    internal static Ydb.Value PackUint16(ushort value) => new() { Uint32Value = value };
    internal static ushort UnpackUint16(this Ydb.Value value) => (ushort)value.Uint32Value;

    internal static Ydb.Value PackUint32(uint value) => new() { Uint32Value = value };
    internal static uint UnpackUint32(this Ydb.Value value) => value.Uint32Value;

    internal static Ydb.Value PackUint64(ulong value) => new() { Uint64Value = value };
    internal static ulong UnpackUint64(this Ydb.Value value) => value.Uint64Value;

    internal static Ydb.Value PackFloat(float value) => new() { FloatValue = value };
    internal static float UnpackFloat(this Ydb.Value value) => value.FloatValue;

    internal static Ydb.Value PackDouble(double value) => new() { DoubleValue = value };
    internal static double UnpackDouble(this Ydb.Value value) => value.DoubleValue;

    internal static Ydb.Value PackDecimal(this decimal value, byte precision, byte scale)
    {
        if (precision == 0 && scale == 0)
        {
            precision = YdbTypeExtensions.DefaultDecimalPrecision;
            scale = YdbTypeExtensions.DefaultDecimalScale;
        }

        if (scale > precision)
            throw new ArgumentOutOfRangeException(nameof(scale), "Scale cannot exceed precision");

        var origScale = (decimal.GetBits(value)[3] >> 16) & 0xFF;

        if (origScale > scale || (precision < MaxPrecisionDecimal && Pow10[precision - scale] <= Math.Abs(value)))
        {
            throw new OverflowException($"Value {value} does not fit Decimal({precision}, {scale})");
        }

        value *= 1.0000000000000000000000000000m; // 28 zeros, max supported by c# decimal
        value = Math.Round(value, scale);
        var bits = decimal.GetBits(value);
        var low = ((ulong)(uint)bits[1] << 32) | (uint)bits[0];
        var high = (ulong)(uint)bits[2];
        var isNegative = bits[3] < 0;

        unchecked
        {
            if (isNegative)
            {
                low = ~low + 1UL;
                high = ~high + (low == 0 ? 1UL : 0UL);
            }
        }

        return new Ydb.Value { Low128 = low, High128 = high };
    }

    internal static decimal UnpackDecimal(this Ydb.Value value, uint scale)
    {
        var low = value.Low128;
        var high = value.High128;
        var isNegative = (high & 0x8000_0000_0000_0000UL) != 0;
        unchecked
        {
            if (isNegative)
            {
                low = ~low + 1UL;
                high = ~high + (low == 0 ? 1UL : 0UL);
            }
        }

        if (high >> 32 != 0)
            throw new OverflowException("Value does not fit into decimal");

        return new decimal((int)low, (int)(low >> 32), (int)high, isNegative, (byte)scale);
    }

    internal static Ydb.Value PackBytes(byte[] value) => new() { BytesValue = ByteString.CopyFrom(value) };
    internal static byte[] UnpackBytes(this Ydb.Value value) => value.BytesValue.ToByteArray();

    internal static Ydb.Value PackText(string value) => new() { TextValue = value };
    internal static string UnpackText(this Ydb.Value value) => value.TextValue;

    internal static Ydb.Value PackUuid(Guid value)
    {
        var bytes = value.ToByteArray();
        var low = BitConverter.ToUInt64(bytes, 0);
        var high = BitConverter.ToUInt64(bytes, 8);

        return new Ydb.Value { Low128 = low, High128 = high };
    }

    internal static Guid UnpackUuid(this Ydb.Value value)
    {
        var high = value.High128;
        var low = value.Low128;

        var lowBytes = BitConverter.GetBytes(low);
        var highBytes = BitConverter.GetBytes(high);

        var guidBytes = new byte[16];
        Array.Copy(lowBytes, 0, guidBytes, 0, 8);
        Array.Copy(highBytes, 0, guidBytes, 8, 8);

        return new Guid(guidBytes);
    }

    internal static Ydb.Value PackDate(DateTime value) =>
        new() { Uint32Value = checked((uint)(value.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerDay)) };

    internal static DateTime UnpackDate(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Uint32Value * TimeSpan.TicksPerDay);

    internal static Ydb.Value PackDate32(DateTime value) => new()
        { Int32Value = (int)(value.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerDay) };

    internal static DateTime UnpackDate32(this Ydb.Value value) =>
        UnixEpoch.AddTicks(checked(value.Int32Value * TimeSpan.TicksPerDay));

    internal static Ydb.Value PackDatetime(DateTime value) => new()
        { Uint32Value = checked((uint)(value.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond)) };

    internal static DateTime UnpackDatetime(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Uint32Value * TimeSpan.TicksPerSecond);

    internal static Ydb.Value PackDatetime64(DateTime value) => new()
        { Int64Value = value.Subtract(DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond };

    internal static DateTime UnpackDatetime64(this Ydb.Value value) =>
        UnixEpoch.AddTicks(checked(value.Int64Value * TimeSpan.TicksPerSecond));

    internal static Ydb.Value PackTimestamp(DateTime value) => new()
        { Uint64Value = checked((ulong)(value.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpanUtils.TicksPerMicrosecond) };

    internal static DateTime UnpackTimestamp(this Ydb.Value value) =>
        UnixEpoch.AddTicks((long)(value.Uint64Value * TimeSpanUtils.TicksPerMicrosecond));

    internal static Ydb.Value PackTimestamp64(DateTime value) => new()
        { Int64Value = (value.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpanUtils.TicksPerMicrosecond };

    internal static DateTime UnpackTimestamp64(this Ydb.Value value) =>
        UnixEpoch.AddTicks(checked(value.Int64Value * TimeSpanUtils.TicksPerMicrosecond));

    internal static Ydb.Value PackInterval(TimeSpan value) => new()
        { Int64Value = value.Ticks / TimeSpanUtils.TicksPerMicrosecond };

    internal static TimeSpan UnpackInterval(this Ydb.Value value) =>
        TimeSpan.FromTicks(value.Int64Value * TimeSpanUtils.TicksPerMicrosecond);

    internal static Ydb.Value PackInterval64(TimeSpan value) => new()
        { Int64Value = value.Ticks / TimeSpanUtils.TicksPerMicrosecond };

    internal static TimeSpan UnpackInterval64(this Ydb.Value value) =>
        TimeSpan.FromTicks(checked(value.Int64Value * TimeSpanUtils.TicksPerMicrosecond));
}
