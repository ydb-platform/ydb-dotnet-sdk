namespace Ydb.Sdk.Value;

public partial class YdbValue
{
    public static explicit operator bool(YdbValue value) => GetObject<bool>(value);

    public static explicit operator bool?(YdbValue value) => GetOptionalPrimitive<bool>(value);

    public static explicit operator sbyte(YdbValue value) => GetObject<sbyte>(value);

    public static explicit operator sbyte?(YdbValue value) => GetOptionalPrimitive<sbyte>(value);

    public static explicit operator byte(YdbValue value) => GetObject<byte>(value);

    public static explicit operator byte?(YdbValue value) => GetOptionalPrimitive<byte>(value);

    public static explicit operator short(YdbValue value) => GetObject<short>(value);

    public static explicit operator short?(YdbValue value) => GetOptionalPrimitive<short>(value);

    public static explicit operator ushort(YdbValue value) => GetObject<ushort>(value);

    public static explicit operator ushort?(YdbValue value) => GetOptionalPrimitive<ushort>(value);

    public static explicit operator int(YdbValue value) => GetObject<int>(value);

    public static explicit operator int?(YdbValue value) => GetOptionalPrimitive<int>(value);

    public static explicit operator uint(YdbValue value) => GetObject<uint>(value);

    public static explicit operator uint?(YdbValue value) => GetOptionalPrimitive<uint>(value);

    public static explicit operator long(YdbValue value) => GetObject<long>(value);

    public static explicit operator long?(YdbValue value) => GetOptionalPrimitive<long>(value);

    public static explicit operator ulong(YdbValue value) => GetObject<ulong>(value);

    public static explicit operator ulong?(YdbValue value) => GetOptionalPrimitive<ulong>(value);

    public static explicit operator float(YdbValue value) => GetObject<float>(value);

    public static explicit operator float?(YdbValue value) => GetOptionalPrimitive<float>(value);

    public static explicit operator double(YdbValue value) => GetObject<double>(value);

    public static explicit operator double?(YdbValue value) => GetOptionalPrimitive<double>(value);

    public static explicit operator DateTime(YdbValue value) => GetObject<DateTime>(value);

    public static explicit operator DateTime?(YdbValue value) => GetOptionalPrimitive<DateTime>(value);

    public static explicit operator TimeSpan(YdbValue value) => GetObject<TimeSpan>(value);

    public static explicit operator TimeSpan?(YdbValue value) => GetOptionalPrimitive<TimeSpan>(value);

    public static explicit operator string?(YdbValue value) => GetOptionalObject<string>(value);

    public static explicit operator byte[]?(YdbValue value) => GetOptionalObject<byte[]>(value);

    public static explicit operator decimal(YdbValue value) => GetObject<decimal>(value);

    public static explicit operator decimal?(YdbValue value) => GetOptionalPrimitive<decimal>(value);

    public static explicit operator Guid?(YdbValue value) => GetOptionalPrimitive<Guid>(value);

    private static T? GetOptionalPrimitive<T>(YdbValue value) where T : struct =>
        value.TypeId == YdbTypeId.OptionalType
            ? value.GetOptional() is not null ? GetObject<T>(value.GetOptional()!) : null
            : GetObject<T>(value);

    private static T? GetOptionalObject<T>(YdbValue value) where T : class =>
        value.TypeId == YdbTypeId.OptionalType
            ? value.GetOptional() is not null ? GetObject<T>(value.GetOptional()!) : null
            : GetObject<T>(value);

    private static T GetObject<T>(YdbValue value) =>
        (T)(object)(value.TypeId switch
        {
            YdbTypeId.Bool => value.GetBool(),
            YdbTypeId.Int8 => value.GetInt8(),
            YdbTypeId.Uint8 => value.GetUint8(),
            YdbTypeId.Int16 => value.GetInt16(),
            YdbTypeId.Uint16 => value.GetUint16(),
            YdbTypeId.Int32 => value.GetInt32(),
            YdbTypeId.Uint32 => value.GetUint32(),
            YdbTypeId.Int64 => value.GetInt64(),
            YdbTypeId.Uint64 => value.GetUint64(),
            YdbTypeId.Float => value.GetFloat(),
            YdbTypeId.Double => value.GetDouble(),
            YdbTypeId.Date => value.GetDate(),
            YdbTypeId.Datetime => value.GetDatetime(),
            YdbTypeId.Timestamp => value.GetTimestamp(),
            YdbTypeId.Interval => value.GetInterval(),
            YdbTypeId.String => value.GetString(),
            YdbTypeId.Utf8 => value.GetUtf8(),
            YdbTypeId.Yson => value.GetYson(),
            YdbTypeId.Json => value.GetJson(),
            YdbTypeId.JsonDocument => value.GetJsonDocument(),
            YdbTypeId.DecimalType => value.GetDecimal(),
            YdbTypeId.Uuid => value.GetUuid(),
            _ => throw new InvalidCastException($"Cannot cast YDB type {value.TypeId} to {typeof(T).Name}.")
        });

    public static explicit operator YdbValue(bool value) => MakeBool(value);

    public static explicit operator YdbValue(bool? value) => MakeOptionalBool(value);

    public static explicit operator YdbValue(sbyte value) => MakeInt8(value);

    public static explicit operator YdbValue(sbyte? value) => MakeOptionalInt8(value);

    public static explicit operator YdbValue(byte value) => MakeUint8(value);

    public static explicit operator YdbValue(byte? value) => MakeOptionalUint8(value);

    public static explicit operator YdbValue(short value) => MakeInt16(value);

    public static explicit operator YdbValue(short? value) => MakeOptionalInt16(value);

    public static explicit operator YdbValue(ushort value) => MakeUint16(value);

    public static explicit operator YdbValue(ushort? value) => MakeOptionalUint16(value);

    public static explicit operator YdbValue(int value) => MakeInt32(value);

    public static explicit operator YdbValue(int? value) => MakeOptionalInt32(value);

    public static explicit operator YdbValue(uint value) => MakeUint32(value);

    public static explicit operator YdbValue(uint? value) => MakeOptionalUint32(value);

    public static explicit operator YdbValue(long value) => MakeInt64(value);

    public static explicit operator YdbValue(long? value) => MakeOptionalInt64(value);

    public static explicit operator YdbValue(ulong value) => MakeUint64(value);

    public static explicit operator YdbValue(ulong? value) => MakeOptionalUint64(value);

    public static explicit operator YdbValue(float value) => MakeFloat(value);

    public static explicit operator YdbValue(float? value) => MakeOptionalFloat(value);

    public static explicit operator YdbValue(double value) => MakeDouble(value);

    public static explicit operator YdbValue(double? value) => MakeOptionalDouble(value);

    public static explicit operator YdbValue(TimeSpan value) => MakeInterval(value);

    public static explicit operator YdbValue(TimeSpan? value) => MakeOptionalInterval(value);

    public static explicit operator YdbValue(decimal value) => MakeDecimal(value);

    public static explicit operator YdbValue(decimal? value) => MakeOptionalDecimal(value);
}
