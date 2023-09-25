namespace Ydb.Sdk.Value;

public enum YdbTypeId : uint
{
    Unknown = 0,

    Bool = 0x0006,
    Int8 = 0x0007,
    Uint8 = 0x0005,
    Int16 = 0x0008,
    Uint16 = 0x0009,
    Int32 = 0x0001,
    Uint32 = 0x0002,
    Int64 = 0x0003,
    Uint64 = 0x0004,
    Float = 0x0021,
    Double = 0x0020,
    Date = 0x0030,
    Datetime = 0x0031,
    Timestamp = 0x0032,
    Interval = 0x0033,
    TzDate = 0x0034,
    TzDatetime = 0x0035,
    TzTimestamp = 0x0036,
    String = 0x1001,
    Utf8 = 0x1200,
    Yson = 0x1201,
    Json = 0x1202,
    Uuid = 0x1203,
    JsonDocument = 0x1204,
    Dynumber = 0x1302,

    DecimalType = YdbTypeIdRanges.ComplexTypesFirst + 2,

    OptionalType = YdbTypeIdRanges.ComplexTypesFirst + 101,
    ListType = YdbTypeIdRanges.ComplexTypesFirst + 102,
    TupleType = YdbTypeIdRanges.ComplexTypesFirst + 103,
    StructType = YdbTypeIdRanges.ComplexTypesFirst + 104,
    DictType = YdbTypeIdRanges.ComplexTypesFirst + 105,
    VariantType = YdbTypeIdRanges.ComplexTypesFirst + 106,

    VoidType = YdbTypeIdRanges.ComplexTypesFirst + 201
}

internal static class YdbTypeIdRanges
{
    public const uint ComplexTypesFirst = 0xffff;
}

public sealed partial class YdbValue
{
    private readonly Type _protoType;
    private readonly Ydb.Value _protoValue;

    internal YdbValue(Type type, Ydb.Value value)
    {
        _protoType = type;
        _protoValue = value;

        TypeId = GetYdbTypeId(type);
    }

    public YdbTypeId TypeId { get; }

    public TypedValue GetProto()
    {
        return new TypedValue
        {
            Type = _protoType,
            Value = _protoValue
        };
    }

    public override string ToString()
    {
        return _protoValue.ToString() ?? "";
    }

    private static YdbTypeId GetYdbTypeId(Type protoType)
    {
        switch (protoType.TypeCase)
        {
            case Type.TypeOneofCase.TypeId:
                return Enum.IsDefined(typeof(YdbTypeId), (uint)protoType.TypeId)
                    ? (YdbTypeId)protoType.TypeId
                    : YdbTypeId.Unknown;

            case Type.TypeOneofCase.DecimalType:
                return YdbTypeId.DecimalType;

            case Type.TypeOneofCase.OptionalType:
                return YdbTypeId.OptionalType;

            case Type.TypeOneofCase.ListType:
                return YdbTypeId.ListType;

            case Type.TypeOneofCase.TupleType:
                return YdbTypeId.TupleType;

            case Type.TypeOneofCase.StructType:
                return YdbTypeId.StructType;

            case Type.TypeOneofCase.DictType:
                return YdbTypeId.DictType;

            case Type.TypeOneofCase.VariantType:
                return YdbTypeId.VariantType;

            case Type.TypeOneofCase.VoidType:
                return YdbTypeId.VoidType;

            default:
                return YdbTypeId.Unknown;
        }
    }
}