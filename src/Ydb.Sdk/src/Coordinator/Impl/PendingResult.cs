using Ydb.Coordination;

namespace Ydb.Sdk.Coordinator.Impl;

public record PendingResult(SessionResponse Request, SessionResponse.ResponseOneofCase EnumResponse);
