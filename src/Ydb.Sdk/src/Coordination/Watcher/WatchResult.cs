namespace Ydb.Sdk.Coordination.Watcher;

public class WatchResult<T>
{
    public T Initial { get; }
    public IAsyncEnumerable<T> Updates { get; }

    public WatchResult(T initial, IAsyncEnumerable<T> updates)
    {
        Initial = initial;
        Updates = updates;
    }
}
