namespace Ydb.Sdk.Ado.Internal;

internal static class ThrowHelper
{
    internal static IndexOutOfRangeException IndexOutOfRangeException(int columnCount) =>
        new("Ordinal must be between 0 and " + (columnCount - 1));

    internal static InvalidCastException InvalidCastException(Type.Types.PrimitiveTypeId expectedType, Type actualType)
        => new($"Invalid type of YDB value, expected: {expectedType}, actual: {actualType}.");

    internal static InvalidCastException InvalidCastException(Type.TypeOneofCase expectedType, Type actualType)
        => new($"Invalid type of YDB value, expected: {expectedType}, actual: {actualType}.");

    internal static InvalidCastException InvalidCastException<T>(Type type) =>
        new($"Field YDB type {type} can't be cast to {typeof(T)} type.");
}
