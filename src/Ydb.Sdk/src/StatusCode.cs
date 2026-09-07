namespace Ydb.Sdk;

internal static class StatusRanges
{
    public const int ClientTransportFirst = 600000;
}

internal static class StatusCodeExtensions
{
    internal static bool IsTransportError(this StatusCode statusCode) =>
        (int)statusCode >= StatusRanges.ClientTransportFirst;
}

public enum StatusCode
{
    Unspecified = 0,
    Success = 400000,
    BadRequest = 400010,
    Unauthorized = 400020,
    InternalError = 400030,
    Aborted = 400040,
    Unavailable = 400050,
    Overloaded = 400060,
    SchemeError = 400070,
    GenericError = 400080,
    Timeout = 400090,
    BadSession = 400100,
    PreconditionFailed = 400120,
    AlreadyExists = 400130,
    NotFound = 400140,
    SessionExpired = 400150,
    Cancelled = 400160,
    Undetermined = 400170,
    Unsupported = 400180,
    SessionBusy = 400190,

    ClientTransportUnknown = StatusRanges.ClientTransportFirst + 10,
    ClientTransportUnavailable = StatusRanges.ClientTransportFirst + 20,
    ClientTransportTimeout = StatusRanges.ClientTransportFirst + 30,
    ClientTransportResourceExhausted = StatusRanges.ClientTransportFirst + 40,
    ClientTransportUnimplemented = StatusRanges.ClientTransportFirst + 50,
    ClientCancelled = StatusRanges.ClientTransportFirst + 60
}
