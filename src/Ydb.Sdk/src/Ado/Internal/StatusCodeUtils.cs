using Ydb.Issue;

namespace Ydb.Sdk.Ado.Internal;

public static class StatusCodeUtils
{
    internal static StatusCode Code(this Grpc.Core.Status rpcStatus) => rpcStatus.StatusCode switch
    {
        Grpc.Core.StatusCode.Unavailable => StatusCode.ClientTransportUnavailable,
        Grpc.Core.StatusCode.DeadlineExceeded or Grpc.Core.StatusCode.Cancelled => StatusCode.ClientTransportTimeout,
        Grpc.Core.StatusCode.ResourceExhausted => StatusCode.ClientTransportResourceExhausted,
        Grpc.Core.StatusCode.Unimplemented => StatusCode.ClientTransportUnimplemented,
        _ => StatusCode.ClientTransportUnknown
    };

    internal static StatusCode Code(this StatusIds.Types.StatusCode statusCode) =>
        Enum.IsDefined(typeof(StatusCode), (int)statusCode) ? (StatusCode)statusCode : StatusCode.Unavailable;

    internal static bool IsNotSuccess(this StatusIds.Types.StatusCode code) =>
        code != StatusIds.Types.StatusCode.Success;

    internal static string ToMessage(this StatusCode statusCode, IReadOnlyList<IssueMessage> issueMessages) =>
        issueMessages.Count == 0
            ? $"StatusCode: {statusCode}"
            : $"StatusCode: {statusCode}, Issues:{Environment.NewLine}{issueMessages.IssuesToString()}";
}
