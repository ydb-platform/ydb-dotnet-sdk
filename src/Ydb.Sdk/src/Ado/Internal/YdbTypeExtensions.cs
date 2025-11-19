namespace Ydb.Sdk.Ado.Internal;

internal static class YdbTypeExtensions
{
    internal const byte DefaultDecimalPrecision = 22;
    internal const byte DefaultDecimalScale = 9;

    private static readonly Type DefaultDecimalType = DecimalType(DefaultDecimalPrecision, DefaultDecimalScale);

    internal static Type DecimalType(byte precision, byte scale) => precision == 0 && scale == 0
        ? DefaultDecimalType
        : new Type { DecimalType = new DecimalType { Precision = precision, Scale = scale } };

    internal static Type ListType(this Type type) => new() { ListType = new ListType { Item = type } };

    internal static Type OptionalType(this Type type) => new() { OptionalType = new OptionalType { Item = type } };
}
