using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Impl;

public record PendingResult(SessionResponse Request, SessionResponse.ResponseOneofCase EnumResponse);
