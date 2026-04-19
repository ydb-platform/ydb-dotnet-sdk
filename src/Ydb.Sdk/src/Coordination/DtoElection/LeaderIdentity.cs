namespace Ydb.Sdk.Coordination.DtoElection;

public record LeaderIdentity(ulong SessionId, ulong OrderId);
