namespace Ydb.Sdk.Coordination.Dto;

public record LeaderState(byte[] Data, bool IsMe, CancellationToken Cancellation);
