using System.Data.Common;
using Grpc.Core;
using Ydb.Issue;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado;

public class YdbException : DbException
{
    internal YdbException(string message) : base(message)
    {
    }

    internal YdbException(RpcException e) : this(e.Status.Code(), "Transport RPC call error", e)
    {
    }

    internal static YdbException FromServer(StatusIds.Types.StatusCode statusCode, IReadOnlyList<IssueMessage> issues)
    {
        var code = statusCode.Code();
        var message = code.ToMessage(issues);

        return new YdbException(code, message);
    }

    internal YdbException(StatusCode statusCode, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = statusCode;
        IsTransient = statusCode.IsTransient();
        IsTransientWhenIdempotent = statusCode.IsTransientWhenIdempotent();
        // TODO: Add SQLSTATE message with order with https://en.wikipedia.org/wiki/SQLSTATE
    }

    public override bool IsTransient { get; }

    public bool IsTransientWhenIdempotent { get; }

    public StatusCode Code { get; }
}

public class YdbOperationInProgressException : InvalidOperationException
{
    public YdbOperationInProgressException(YdbConnection ydbConnection)
        : base("A command is already in progress: " + ydbConnection.LastCommand)
    {
    }
}
