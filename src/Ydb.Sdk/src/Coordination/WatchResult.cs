namespace Ydb.Sdk.Coordination;

/// <summary>
/// Initial snapshot + stream of subsequent updates returned by a watch subscription.
/// </summary>
public sealed record WatchResult<T>(T Initial, IAsyncEnumerable<T> Updates);
