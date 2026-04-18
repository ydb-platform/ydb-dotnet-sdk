namespace Ydb.Sdk.Coordination.Dto;

public record LeaderIdentity(ulong SessionId, ulong OrderId);
