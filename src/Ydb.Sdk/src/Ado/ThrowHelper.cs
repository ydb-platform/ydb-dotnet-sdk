using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado;

internal static class ThrowHelper
{
    internal static T ThrowInvalidCast<T>(YdbValue ydbValue)
    {
        throw new InvalidCastException($"Field type {ydbValue.TypeId} can't be cast to {typeof(T)} type.");
    }
}
