namespace Ydb.Sdk.Value;

public partial class YdbValue
{
    public static explicit operator bool(YdbValue value)
    {
        return GetObject<bool>(value);
    }

    public static explicit operator bool?(YdbValue value)
    {
        return GetOptionalPrimitive<bool>(value);
    }

    public static explicit operator sbyte(YdbValue value)
    {
        return GetObject<sbyte>(value);
    }

    public static explicit operator sbyte?(YdbValue value)
    {
        return GetOptionalPrimitive<sbyte>(value);
    }

    public static explicit operator byte(YdbValue value)
    {
        return GetObject<byte>(value);
    }

    public static explicit operator byte?(YdbValue value)
    {
        return GetOptionalPrimitive<byte>(value);
    }

    public static explicit operator short(YdbValue value)
    {
        return GetObject<short>(value);
    }

    public static explicit operator short?(YdbValue value)
    {
        return GetOptionalPrimitive<short>(value);
    }

    public static explicit operator ushort(YdbValue value)
    {
        return GetObject<ushort>(value);
    }

    public static explicit operator ushort?(YdbValue value)
    {
        return GetOptionalPrimitive<ushort>(value);
    }

    public static explicit operator int(YdbValue value)
    {
        return GetObject<int>(value);
    }

    public static explicit operator int?(YdbValue value)
    {
        return GetOptionalPrimitive<int>(value);
    }

    public static explicit operator uint(YdbValue value)
    {
        return GetObject<uint>(value);
    }

    public static explicit operator uint?(YdbValue value)
    {
        return GetOptionalPrimitive<uint>(value);
    }

    public static explicit operator long(YdbValue value)
    {
        return GetObject<long>(value);
    }

    public static explicit operator long?(YdbValue value)
    {
        return GetOptionalPrimitive<long>(value);
    }

    public static explicit operator ulong(YdbValue value)
    {
        return GetObject<ulong>(value);
    }

    public static explicit operator ulong?(YdbValue value)
    {
        return GetOptionalPrimitive<ulong>(value);
    }

    public static explicit operator float(YdbValue value)
    {
        return GetObject<float>(value);
    }

    public static explicit operator float?(YdbValue value)
    {
        return GetOptionalPrimitive<float>(value);
    }

    public static explicit operator double(YdbValue value)
    {
        return GetObject<double>(value);
    }

    public static explicit operator double?(YdbValue value)
    {
        return GetOptionalPrimitive<double>(value);
    }

    public static explicit operator DateTime(YdbValue value)
    {
        return GetObject<DateTime>(value);
    }

    public static explicit operator DateTime?(YdbValue value)
    {
        return GetOptionalPrimitive<DateTime>(value);
    }

    public static explicit operator TimeSpan(YdbValue value)
    {
        return GetObject<TimeSpan>(value);
    }

    public static explicit operator TimeSpan?(YdbValue value)
    {
        return GetOptionalPrimitive<TimeSpan>(value);
    }

    public static explicit operator string?(YdbValue value)
    {
        return GetOptionalObject<string>(value);
    }

    public static explicit operator byte[]?(YdbValue value)
    {
        return GetOptionalObject<byte[]>(value);
    }

    public static explicit operator decimal(YdbValue value)
    {
        return GetObject<decimal>(value);
    }

    public static explicit operator decimal?(YdbValue value)
    {
        return GetOptionalPrimitive<decimal>(value);
    }

    public static explicit operator Guid?(YdbValue value)
    {
        return GetOptionalPrimitive<Guid>(value);
    }

    private static T? GetOptionalPrimitive<T>(YdbValue value) where T : struct
    {
        return value.TypeId == YdbTypeId.OptionalType
            ? value.GetOptional() is not null ? GetObject<T>(value.GetOptional()!) : null
            : GetObject<T>(value);
    }

    private static T? GetOptionalObject<T>(YdbValue value) where T : class
    {
        return value.TypeId == YdbTypeId.OptionalType
            ? value.GetOptional() is not null ? GetObject<T>(value.GetOptional()!) : null
            : GetObject<T>(value);
    }

    private static T GetObject<T>(YdbValue value)
    {
        return (T)(object)(value.TypeId switch
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
    }

    public static explicit operator YdbValue(bool value)
    {
        return MakeBool(value);
    }

    public static explicit operator YdbValue(bool? value)
    {
        return MakeOptionalBool(value);
    }

    public static explicit operator YdbValue(sbyte value)
    {
        return MakeInt8(value);
    }

    public static explicit operator YdbValue(sbyte? value)
    {
        return MakeOptionalInt8(value);
    }

    public static explicit operator YdbValue(byte value)
    {
        return MakeUint8(value);
    }

    public static explicit operator YdbValue(byte? value)
    {
        return MakeOptionalUint8(value);
    }

    public static explicit operator YdbValue(short value)
    {
        return MakeInt16(value);
    }

    public static explicit operator YdbValue(short? value)
    {
        return MakeOptionalInt16(value);
    }

    public static explicit operator YdbValue(ushort value)
    {
        return MakeUint16(value);
    }

    public static explicit operator YdbValue(ushort? value)
    {
        return MakeOptionalUint16(value);
    }

    public static explicit operator YdbValue(int value)
    {
        return MakeInt32(value);
    }

    public static explicit operator YdbValue(int? value)
    {
        return MakeOptionalInt32(value);
    }

    public static explicit operator YdbValue(uint value)
    {
        return MakeUint32(value);
    }

    public static explicit operator YdbValue(uint? value)
    {
        return MakeOptionalUint32(value);
    }

    public static explicit operator YdbValue(long value)
    {
        return MakeInt64(value);
    }

    public static explicit operator YdbValue(long? value)
    {
        return MakeOptionalInt64(value);
    }

    public static explicit operator YdbValue(ulong value)
    {
        return MakeUint64(value);
    }

    public static explicit operator YdbValue(ulong? value)
    {
        return MakeOptionalUint64(value);
    }

    public static explicit operator YdbValue(float value)
    {
        return MakeFloat(value);
    }

    public static explicit operator YdbValue(float? value)
    {
        return MakeOptionalFloat(value);
    }

    public static explicit operator YdbValue(double value)
    {
        return MakeDouble(value);
    }

    public static explicit operator YdbValue(double? value)
    {
        return MakeOptionalDouble(value);
    }

    public static explicit operator YdbValue(TimeSpan value)
    {
        return MakeInterval(value);
    }

    public static explicit operator YdbValue(TimeSpan? value)
    {
        return MakeOptionalInterval(value);
    }

    public static explicit operator YdbValue(decimal value)
    {
        return MakeDecimal(value);
    }

    public static explicit operator YdbValue(decimal? value)
    {
        return MakeOptionalDecimal(value);
    }
}
