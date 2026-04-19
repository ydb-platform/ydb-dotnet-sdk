namespace Ydb.Sdk.Coordination.DtoElection;

public record LeaderState(byte[] Data, bool IsMe, CancellationToken Cancellation);
