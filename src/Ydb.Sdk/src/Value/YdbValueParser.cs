﻿namespace Ydb.Sdk.Value;

public partial class YdbValue
{
    public bool GetBool()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Bool);
        return _protoValue.BoolValue;
    }

    public sbyte GetInt8()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Int8);
        return (sbyte)_protoValue.Int32Value;
    }

    public byte GetUint8()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Uint8);
        return (byte)_protoValue.Uint32Value;
    }

    public short GetInt16()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Int16);
        return (short)_protoValue.Int32Value;
    }

    public ushort GetUint16()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Uint16);
        return (ushort)_protoValue.Uint32Value;
    }

    public int GetInt32()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Int32);
        return _protoValue.Int32Value;
    }

    public uint GetUint32()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Uint32);
        return _protoValue.Uint32Value;
    }

    public long GetInt64()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Int64);
        return _protoValue.Int64Value;
    }

    public ulong GetUint64()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Uint64);
        return _protoValue.Uint64Value;
    }

    public float GetFloat()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Float);
        return _protoValue.FloatValue;
    }

    public double GetDouble()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Double);
        return _protoValue.DoubleValue;
    }

    public DateTime GetDate()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Date);
        var secondsSinceEpoch = (long)TimeSpan.FromDays(_protoValue.Uint32Value).TotalSeconds;
        return DateTimeOffset.FromUnixTimeSeconds(secondsSinceEpoch).Date;
    }

    public DateTime GetDatetime()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Datetime);
        return DateTimeOffset.FromUnixTimeSeconds(_protoValue.Uint32Value).DateTime;
    }

    public DateTime GetTimestamp()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Timestamp);
        var us = _protoValue.Uint64Value;
        var ms = us / 1000;
        var ticks = us % 1000 * TimeSpan.TicksPerMillisecond / 1000;

        return DateTimeOffset.FromUnixTimeMilliseconds((long)ms).AddTicks((long)ticks).DateTime;
    }

    public TimeSpan GetInterval()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Interval);
        return TimeSpan.FromMilliseconds((double)_protoValue.Int64Value / 1000);
    }

    public byte[] GetString()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.String);
        return _protoValue.BytesValue.ToByteArray();
    }

    public string GetUtf8()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Utf8);
        return _protoValue.TextValue;
    }

    public byte[] GetYson()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Yson);
        return _protoValue.BytesValue.ToByteArray();
    }

    public string GetJson()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Json);
        return _protoValue.TextValue;
    }

    public string GetJsonDocument()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.JsonDocument);
        return _protoValue.TextValue;
    }

    public decimal GetDecimal()
    {
        EnsureType(Type.TypeOneofCase.DecimalType);
        var low64 = _protoValue.Low128;
        var high64 = _protoValue.High128;

        var scale = _protoType.DecimalType.Scale;

        var isNegative = false;

        unchecked
        {
            if (high64 >> 63 == 1) // if negative
            {
                isNegative = true;
                if (low64 == 0)
                {
                    high64 -= 1;
                }

                low64 -= 1;

                low64 = ~low64;
                high64 = ~high64;
            }
        }

        return new decimal((int)low64, (int)(low64 >> 32), (int)high64, isNegative, (byte)scale);
    }

    public bool? GetOptionalBool()
    {
        return GetOptional()?.GetBool();
    }


    public sbyte? GetOptionalInt8()
    {
        return GetOptional()?.GetInt8();
    }

    public byte? GetOptionalUint8()
    {
        return GetOptional()?.GetUint8();
    }

    public short? GetOptionalInt16()
    {
        return GetOptional()?.GetInt16();
    }

    public ushort? GetOptionalUint16()
    {
        return GetOptional()?.GetUint16();
    }

    public int? GetOptionalInt32()
    {
        return GetOptional()?.GetInt32();
    }

    public uint? GetOptionalUint32()
    {
        return GetOptional()?.GetUint32();
    }

    public long? GetOptionalInt64()
    {
        return GetOptional()?.GetInt64();
    }

    public ulong? GetOptionalUint64()
    {
        return GetOptional()?.GetUint64();
    }

    public float? GetOptionalFloat()
    {
        return GetOptional()?.GetFloat();
    }

    public double? GetOptionalDouble()
    {
        return GetOptional()?.GetDouble();
    }

    public DateTime? GetOptionalDate()
    {
        return GetOptional()?.GetDate();
    }

    public DateTime? GetOptionalDatetime()
    {
        return GetOptional()?.GetDatetime();
    }

    public DateTime? GetOptionalTimestamp()
    {
        return GetOptional()?.GetTimestamp();
    }

    public TimeSpan? GetOptionalInterval()
    {
        return GetOptional()?.GetInterval();
    }

    public byte[]? GetOptionalString()
    {
        return GetOptional()?.GetString();
    }

    public string? GetOptionalUtf8()
    {
        return GetOptional()?.GetUtf8();
    }

    public byte[]? GetOptionalYson()
    {
        return GetOptional()?.GetYson();
    }

    public string? GetOptionalJson()
    {
        return GetOptional()?.GetJson();
    }

    public string? GetOptionalJsonDocument()
    {
        return GetOptional()?.GetJsonDocument();
    }

    public decimal? GetOptionalDecimal()
    {
        return GetOptional()?.GetDecimal();
    }

    public YdbValue? GetOptional()
    {
        EnsureType(Type.TypeOneofCase.OptionalType);

        switch (_protoValue.ValueCase)
        {
            case Ydb.Value.ValueOneofCase.NullFlagValue:
                return null;
            case Ydb.Value.ValueOneofCase.NestedValue:
                return new YdbValue(_protoType.OptionalType.Item, _protoValue.NestedValue);
            default:
                return new YdbValue(_protoType.OptionalType.Item, _protoValue);
        }
    }

    public IReadOnlyList<YdbValue> GetList()
    {
        EnsureType(Type.TypeOneofCase.ListType);
        return _protoValue.Items.Select(item => new YdbValue(_protoType.ListType.Item, item)).ToList();
    }

    public IReadOnlyList<YdbValue> GetTuple()
    {
        EnsureType(Type.TypeOneofCase.TupleType);
        return _protoValue.Items.Select((item, index) => new YdbValue(_protoType.TupleType.Elements[index], item))
            .ToList();
    }

    public IReadOnlyDictionary<string, YdbValue> GetStruct()
    {
        EnsureType(Type.TypeOneofCase.StructType);
        return _protoValue.Items.Select((item, index) => new
        {
            _protoType.StructType.Members[index].Name,
            Value = new YdbValue(_protoType.StructType.Members[index].Type, item)
        }).ToDictionary(p => p.Name, p => p.Value);
    }

    private void EnsureType(Type.TypeOneofCase expectedType)
    {
        if (_protoType.TypeCase != expectedType)
        {
            throw new InvalidTypeException(expectedType.ToString(), TypeId.ToString());
        }
    }

    private void EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId primitiveTypeId)
    {
        if (_protoType.TypeCase != Type.TypeOneofCase.TypeId || _protoType.TypeId != primitiveTypeId)
        {
            throw new InvalidTypeException(primitiveTypeId.ToString(), TypeId.ToString());
        }
    }

    public class InvalidTypeException : Exception
    {
        internal InvalidTypeException(string expectedType, string actualType)
            : base($"Invalid type of YDB value, expected: {expectedType}, actual: {actualType}.")
        {
        }
    }
}
