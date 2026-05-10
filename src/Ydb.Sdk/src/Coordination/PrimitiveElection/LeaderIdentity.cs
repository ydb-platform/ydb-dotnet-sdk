namespace Ydb.Sdk.Coordination.PrimitiveElection;

public record LeaderIdentity(ulong SessionId, ulong OrderId);
