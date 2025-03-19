using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

internal static class ThrowHelper
{
    internal static T ThrowInvalidCast<T>(YdbValue ydbValue) =>
        throw new InvalidCastException($"Field YDB type {ydbValue.TypeId} can't be cast to {typeof(T)} type.");

    internal static void ThrowIndexOutOfRangeException(int columnCount) =>
        throw new IndexOutOfRangeException("Ordinal must be between 0 and " + (columnCount - 1));

    internal static void ThrowInvalidCastException(string expectedType, string actualType) =>
        throw new InvalidCastException($"Invalid type of YDB value, expected: {expectedType}, actual: {actualType}.");
}
