namespace Ydb.Sdk.Coordination.Recipes;

/// <summary>
/// Snapshot of the currently-elected leader for a named election.
/// </summary>
/// <param name="SessionId">Server-assigned session id of the leader (stable until reconnect).</param>
/// <param name="OrderId">Server-assigned order id — monotonically increases with every acquire,
/// can be used to detect leadership changes.</param>
/// <param name="Data">Payload published by the leader (typically its endpoint).</param>
public sealed record LeaderInfo(ulong SessionId, ulong OrderId, byte[] Data);
