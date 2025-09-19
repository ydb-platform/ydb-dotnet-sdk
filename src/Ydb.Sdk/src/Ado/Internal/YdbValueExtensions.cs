namespace Ydb.Sdk.Ado.Internal;

internal static class YdbValueExtensions
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    internal static bool IsNull(this Ydb.Value value) => value.ValueCase == Ydb.Value.ValueOneofCase.NullFlagValue;

    internal static bool GetBool(this Ydb.Value value) => value.BoolValue;

    internal static sbyte GetInt8(this Ydb.Value value) => (sbyte)value.Int32Value;

    internal static byte GetUint8(this Ydb.Value value) => (byte)value.Uint32Value;

    internal static short GetInt16(this Ydb.Value value) => (short)value.Int32Value;

    internal static ushort GetUint16(this Ydb.Value value) => (ushort)value.Uint32Value;

    internal static int GetInt32(this Ydb.Value value) => value.Int32Value;

    internal static uint GetUint32(this Ydb.Value value) => value.Uint32Value;

    internal static long GetInt64(this Ydb.Value value) => value.Int64Value;

    internal static ulong GetUint64(this Ydb.Value value) => value.Uint64Value;

    internal static float GetFloat(this Ydb.Value value) => value.FloatValue;

    internal static double GetDouble(this Ydb.Value value) => value.DoubleValue;

    internal static DateTime GetDate(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Uint32Value * TimeSpan.TicksPerDay);

    internal static DateTime GetDate32(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Int32Value * TimeSpan.TicksPerDay);

    internal static DateTime GetDatetime(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Uint32Value * TimeSpan.TicksPerSecond);

    internal static DateTime GetDatetime64(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Int64Value * TimeSpan.TicksPerSecond);

    internal static DateTime GetTimestamp(this Ydb.Value value) =>
        UnixEpoch.AddTicks((long)(value.Uint64Value * TimeSpanUtils.TicksPerMicrosecond));

    internal static DateTime GetTimestamp64(this Ydb.Value value) =>
        UnixEpoch.AddTicks(value.Int64Value * TimeSpanUtils.TicksPerMicrosecond);

    internal static TimeSpan GetInterval(this Ydb.Value value) =>
        TimeSpan.FromTicks(value.Int64Value * TimeSpanUtils.TicksPerMicrosecond);

    internal static TimeSpan GetInterval64(this Ydb.Value value) =>
        TimeSpan.FromTicks(value.Int64Value * TimeSpanUtils.TicksPerMicrosecond);

    internal static byte[] GetBytes(this Ydb.Value value) => value.BytesValue.ToByteArray();
    
    internal static byte[] GetYson(this Ydb.Value value) => value.BytesValue.ToByteArray();

    internal static string GetText(this Ydb.Value value) => value.TextValue;

    internal static string GetJson(this Ydb.Value value) => value.TextValue;

    internal static string GetJsonDocument(this Ydb.Value value) => value.TextValue;

    internal static Guid GetUuid(this Ydb.Value value)
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

    internal static decimal GetDecimal(this Ydb.Value value, uint scale)
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
}
