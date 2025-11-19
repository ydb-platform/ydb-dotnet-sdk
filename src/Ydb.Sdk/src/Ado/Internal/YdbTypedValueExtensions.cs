using static Ydb.Sdk.Ado.Internal.YdbTypeExtensions;
using static Ydb.Sdk.Ado.Internal.YdbValueExtensions;

namespace Ydb.Sdk.Ado.Internal;

internal static class YdbTypedValueExtensions
{
    private static readonly TypedValue DecimalDefaultNull = DecimalNull(DefaultDecimalPrecision, DefaultDecimalScale);

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

    internal static TypedValue DecimalNull(byte precision, byte scale) => precision == 0 && scale == 0
        ? DecimalDefaultNull
        : new TypedValue
        {
            Type = new Type { OptionalType = new OptionalType { Item = DecimalType(precision, scale) } },
            Value = YdbValueNull
        };

    internal static TypedValue ListNull(Type type) =>
        new() { Type = type.ListType().OptionalType(), Value = YdbValueNull };
}
