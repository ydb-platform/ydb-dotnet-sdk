using Ydb.Issue;

namespace Ydb.Sdk.Ado.Internal;

public static class StatusCodeUtils
{
    internal static StatusCode Code(this Grpc.Core.Status rpcStatus) => rpcStatus.StatusCode switch
    {
        Grpc.Core.StatusCode.Unavailable => StatusCode.ClientTransportUnavailable,
        Grpc.Core.StatusCode.DeadlineExceeded => StatusCode.ClientTransportTimeout,
        Grpc.Core.StatusCode.ResourceExhausted => StatusCode.ClientTransportResourceExhausted,
        Grpc.Core.StatusCode.Unimplemented => StatusCode.ClientTransportUnimplemented,
        Grpc.Core.StatusCode.Cancelled => StatusCode.Cancelled,
        _ => StatusCode.ClientTransportUnknown
    };

    internal static StatusCode Code(this StatusIds.Types.StatusCode statusCode)
    {
        var value = (uint)statusCode;
        if (Enum.IsDefined(typeof(StatusCode), value))
        {
            return (StatusCode)value;
        }

        return StatusCode.Unspecified;
    }

    internal static bool IsNotSuccess(this StatusIds.Types.StatusCode code) =>
        code != StatusIds.Types.StatusCode.Success;

    internal static string ToMessage(this StatusCode statusCode, IReadOnlyList<IssueMessage> issueMessages) =>
        issueMessages.Count == 0
            ? $"Status: {statusCode}"
            : $"Status: {statusCode}, Issues:{Environment.NewLine}{issueMessages.IssuesToString()}";
}
