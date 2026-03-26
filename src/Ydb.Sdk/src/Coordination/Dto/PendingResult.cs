using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Dto;

public record PendingResult(SessionResponse Request, SessionResponse.ResponseOneofCase EnumResponse);
