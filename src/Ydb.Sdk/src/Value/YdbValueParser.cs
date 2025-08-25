using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Value;

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
        return TimeSpan.FromTicks(_protoValue.Int64Value * (1000 / Duration.NanosecondsPerTick));
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

    public Guid GetUuid()
    {
        EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId.Uuid);

        var high = _protoValue.High128;
        var low = _protoValue.Low128;

        var lowBytes = BitConverter.GetBytes(low);
        var highBytes = BitConverter.GetBytes(high);

        var guidBytes = new byte[16];
        Array.Copy(lowBytes, 0, guidBytes, 0, 8);
        Array.Copy(highBytes, 0, guidBytes, 8, 8);

        return new Guid(guidBytes);
    }

    public decimal GetDecimal()
    {
        EnsureType(Type.TypeOneofCase.DecimalType);
        var lo = _protoValue.Low128;
        var hi = _protoValue.High128;
        var scale = _protoType.DecimalType.Scale;
        var isNegative = (hi & 0x8000_0000_0000_0000UL) != 0;

        unchecked
        {
            if (isNegative)
            {
                if (lo == 0)
                    hi--;

                lo--;
                lo = ~lo;
                hi = ~hi;
            }
        }

        if (hi >> 32 != 0)
            throw new OverflowException("Value does not fit into decimal");

        return new decimal((int)lo, (int)(lo >> 32), (int)hi, isNegative, (byte)scale);
    }

    public bool? GetOptionalBool() => GetOptional()?.GetBool();


    public sbyte? GetOptionalInt8() => GetOptional()?.GetInt8();

    public byte? GetOptionalUint8() => GetOptional()?.GetUint8();

    public short? GetOptionalInt16() => GetOptional()?.GetInt16();

    public ushort? GetOptionalUint16() => GetOptional()?.GetUint16();

    public int? GetOptionalInt32() => GetOptional()?.GetInt32();

    public uint? GetOptionalUint32() => GetOptional()?.GetUint32();

    public long? GetOptionalInt64() => GetOptional()?.GetInt64();

    public ulong? GetOptionalUint64() => GetOptional()?.GetUint64();

    public float? GetOptionalFloat() => GetOptional()?.GetFloat();

    public double? GetOptionalDouble() => GetOptional()?.GetDouble();

    public DateTime? GetOptionalDate() => GetOptional()?.GetDate();

    public DateTime? GetOptionalDatetime() => GetOptional()?.GetDatetime();

    public DateTime? GetOptionalTimestamp() => GetOptional()?.GetTimestamp();

    public TimeSpan? GetOptionalInterval() => GetOptional()?.GetInterval();

    public byte[]? GetOptionalString() => GetOptional()?.GetString();

    public string? GetOptionalUtf8() => GetOptional()?.GetUtf8();

    public byte[]? GetOptionalYson() => GetOptional()?.GetYson();

    public string? GetOptionalJson() => GetOptional()?.GetJson();

    public string? GetOptionalJsonDocument() => GetOptional()?.GetJsonDocument();

    public Guid? GetOptionalUuid() => GetOptional()?.GetUuid();

    public decimal? GetOptionalDecimal() => GetOptional()?.GetDecimal();

    public YdbValue? GetOptional()
    {
        EnsureType(Type.TypeOneofCase.OptionalType);

        return _protoValue.ValueCase switch
        {
            Ydb.Value.ValueOneofCase.NullFlagValue => null,
            Ydb.Value.ValueOneofCase.NestedValue => new YdbValue(_protoType.OptionalType.Item, _protoValue.NestedValue),
            _ => new YdbValue(_protoType.OptionalType.Item, _protoValue)
        };
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
            throw ThrowHelper.InvalidCastException(expectedType, _protoType);
        }
    }

    private void EnsurePrimitiveTypeId(Type.Types.PrimitiveTypeId primitiveTypeId)
    {
        if (_protoType.TypeCase != Type.TypeOneofCase.TypeId || _protoType.TypeId != primitiveTypeId)
        {
            throw ThrowHelper.InvalidCastException(primitiveTypeId, _protoType);
        }
    }
}
