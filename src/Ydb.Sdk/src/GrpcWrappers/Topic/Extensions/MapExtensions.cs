using Google.Protobuf.Collections;

namespace Ydb.Sdk.GrpcWrappers.Topic.Extensions;

internal static class MapExtensions
{
    public static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(this MapField<TKey, TValue> source)
        where TKey : notnull
    {
        return source.ToDictionary(s => s.Key, s => s.Value);
    }
}