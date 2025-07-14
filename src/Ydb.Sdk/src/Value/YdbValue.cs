namespace Ydb.Sdk.Value;

public enum YdbTypeId
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

    VoidType = YdbTypeIdRanges.ComplexTypesFirst + 201,
    Null = YdbTypeIdRanges.ComplexTypesFirst + 202
}

internal static class YdbTypeIdRanges
{
    public const int ComplexTypesFirst = 0xffff;
}

public sealed partial class YdbValue
{
    private readonly Type _protoType;
    private readonly Ydb.Value _protoValue;

    internal YdbValue(Type type, Ydb.Value value)
    {
        _protoType = type;
        _protoValue = value;
    }

    public YdbTypeId TypeId => GetYdbTypeId(_protoType);

    public TypedValue GetProto() => new()
    {
        Type = _protoType,
        Value = _protoValue
    };

    public override string ToString() => _protoValue.ToString() ?? "";

    internal string ToYql() => ToYql(_protoType);

    private static string ToYql(Type type) =>
        type.TypeCase switch
        {
            Type.TypeOneofCase.TypeId => type.TypeId.ToString(),
            Type.TypeOneofCase.DecimalType => "Decimal(22, 9)",
            Type.TypeOneofCase.OptionalType => $"{ToYql(type.OptionalType.Item)}?",
            Type.TypeOneofCase.ListType => $"List<{ToYql(type.ListType.Item)}>",
            Type.TypeOneofCase.VoidType => "Void",
            _ => "Unknown"
        };

    internal static YdbTypeId GetYdbTypeId(Type protoType) =>
        protoType.TypeCase switch
        {
            Type.TypeOneofCase.TypeId => Enum.IsDefined(typeof(YdbTypeId), (int)protoType.TypeId)
                ? (YdbTypeId)protoType.TypeId
                : YdbTypeId.Unknown,
            Type.TypeOneofCase.DecimalType => YdbTypeId.DecimalType,
            Type.TypeOneofCase.OptionalType => YdbTypeId.OptionalType,
            Type.TypeOneofCase.ListType => YdbTypeId.ListType,
            Type.TypeOneofCase.TupleType => YdbTypeId.TupleType,
            Type.TypeOneofCase.StructType => YdbTypeId.StructType,
            Type.TypeOneofCase.DictType => YdbTypeId.DictType,
            Type.TypeOneofCase.VariantType => YdbTypeId.VariantType,
            Type.TypeOneofCase.VoidType => YdbTypeId.VoidType,
            Type.TypeOneofCase.NullType => YdbTypeId.Null,
            _ => YdbTypeId.Unknown
        };
}
