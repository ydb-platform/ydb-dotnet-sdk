namespace Ydb.Sdk.Coordination;

/// <summary>
/// Initial snapshot + stream of subsequent updates returned by a watch subscription.
/// </summary>
public sealed class WatchResult<T>
{
    public T Initial { get; }
    public IAsyncEnumerable<T> Updates { get; }

    public WatchResult(T initial, IAsyncEnumerable<T> updates)
    {
        Initial = initial;
        Updates = updates;
    }
}
