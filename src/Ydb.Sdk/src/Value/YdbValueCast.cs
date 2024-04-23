using System.Diagnostics.CodeAnalysis;

namespace Ydb.Sdk.Value;

public partial class YdbValue
{
    public static explicit operator bool(YdbValue value)
    {
        return (bool)GetObject(value, typeof(bool));
    }

    public static explicit operator bool?(YdbValue value)
    {
        return (bool?)GetOptionalObject(value, typeof(bool));
    }

    public static explicit operator sbyte(YdbValue value)
    {
        return (sbyte)GetObject(value, typeof(sbyte));
    }

    public static explicit operator sbyte?(YdbValue value)
    {
        return (sbyte?)GetOptionalObject(value, typeof(sbyte));
    }

    public static explicit operator byte(YdbValue value)
    {
        return (byte)GetObject(value, typeof(byte));
    }

    public static explicit operator byte?(YdbValue value)
    {
        return (byte?)GetOptionalObject(value, typeof(byte));
    }

    public static explicit operator short(YdbValue value)
    {
        return (short)GetObject(value, typeof(short));
    }

    public static explicit operator short?(YdbValue value)
    {
        return (short?)GetOptionalObject(value, typeof(short));
    }

    public static explicit operator ushort(YdbValue value)
    {
        return (ushort)GetObject(value, typeof(ushort));
    }

    public static explicit operator ushort?(YdbValue value)
    {
        return (ushort?)GetOptionalObject(value, typeof(ushort));
    }

    public static explicit operator int(YdbValue value)
    {
        return (int)GetObject(value, typeof(int));
    }

    public static explicit operator int?(YdbValue value)
    {
        return (int?)GetOptionalObject(value, typeof(int));
    }

    public static explicit operator uint(YdbValue value)
    {
        return (uint)GetObject(value, typeof(uint));
    }

    public static explicit operator uint?(YdbValue value)
    {
        return (uint?)GetOptionalObject(value, typeof(uint));
    }

    public static explicit operator long(YdbValue value)
    {
        return (long)GetObject(value, typeof(long));
    }

    public static explicit operator long?(YdbValue value)
    {
        return (long?)GetOptionalObject(value, typeof(long));
    }

    public static explicit operator ulong(YdbValue value)
    {
        return (ulong)GetObject(value, typeof(ulong));
    }

    public static explicit operator ulong?(YdbValue value)
    {
        return (ulong?)GetOptionalObject(value, typeof(ulong));
    }

    public static explicit operator float(YdbValue value)
    {
        return (float)GetObject(value, typeof(float));
    }

    public static explicit operator float?(YdbValue value)
    {
        return (float?)GetOptionalObject(value, typeof(float));
    }

    public static explicit operator double(YdbValue value)
    {
        return (double)GetObject(value, typeof(double));
    }

    public static explicit operator double?(YdbValue value)
    {
        return (double?)GetOptionalObject(value, typeof(double));
    }

    public static explicit operator DateTime(YdbValue value)
    {
        return (DateTime)GetObject(value, typeof(DateTime));
    }

    public static explicit operator DateTime?(YdbValue value)
    {
        return (DateTime?)GetOptionalObject(value, typeof(DateTime));
    }

    public static explicit operator TimeSpan(YdbValue value)
    {
        return (TimeSpan)GetObject(value, typeof(TimeSpan));
    }

    public static explicit operator TimeSpan?(YdbValue value)
    {
        return (TimeSpan?)GetOptionalObject(value, typeof(TimeSpan));
    }

    public static explicit operator string?(YdbValue value)
    {
        return (string?)GetOptionalObject(value, typeof(string));
    }

    public static explicit operator byte[]?(YdbValue value)
    {
        return (byte[]?)GetOptionalObject(value, typeof(byte[]));
    }

    public static explicit operator decimal(YdbValue value)
    {
        return (decimal)GetObject(value, typeof(decimal));
    }

    public static explicit operator decimal?(YdbValue value)
    {
        return (decimal?)GetOptionalObject(value, typeof(decimal));
    }

    private static object GetObject(YdbValue value, System.Type targetType)
    {
        return GetObjectInternal(value.TypeId, value, targetType);
    }

    private static object? GetOptionalObject(YdbValue value, System.Type targetType)
    {
        return value.TypeId == YdbTypeId.OptionalType
            ? GetObjectInternal(GetYdbTypeId(value._protoType.OptionalType.Item), value.GetOptional(), targetType)
            : GetObject(value, targetType);
    }

    [return: NotNullIfNotNull("value")]
    private static object? GetObjectInternal(YdbTypeId typeId, YdbValue? value, System.Type targetType)
    {
        return typeId switch
        {
            YdbTypeId.Bool => value?.GetBool(),
            YdbTypeId.Int8 => value?.GetInt8(),
            YdbTypeId.Uint8 => value?.GetUint8(),
            YdbTypeId.Int16 => value?.GetInt16(),
            YdbTypeId.Uint16 => value?.GetUint16(),
            YdbTypeId.Int32 => value?.GetInt32(),
            YdbTypeId.Uint32 => value?.GetUint32(),
            YdbTypeId.Int64 => value?.GetInt64(),
            YdbTypeId.Uint64 => value?.GetUint64(),
            YdbTypeId.Float => value?.GetFloat(),
            YdbTypeId.Double => value?.GetDouble(),
            YdbTypeId.Date => value?.GetDate(),
            YdbTypeId.Datetime => value?.GetDatetime(),
            YdbTypeId.Timestamp => value?.GetTimestamp(),
            YdbTypeId.Interval => value?.GetInterval(),
            YdbTypeId.String => value?.GetString(),
            YdbTypeId.Utf8 => value?.GetUtf8(),
            YdbTypeId.Yson => value?.GetYson(),
            YdbTypeId.Json => value?.GetJson(),
            YdbTypeId.JsonDocument => value?.GetJsonDocument(),
            YdbTypeId.DecimalType => value?.GetDecimal(),
            _ => throw new InvalidCastException($"Cannot cast YDB type {typeId} to {targetType.Name}.")
        };
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
