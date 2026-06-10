namespace Ydb.Sdk.Coordination.PrimitiveElection;

public record LeaderState(byte[] Data, bool IsMe, CancellationToken Cancellation);
