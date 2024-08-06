using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbException : DbException
{
    public YdbException(string message) : base(message)
    {
    }

    public YdbException(string message, Exception e) : base(message, e)
    {
    }

    public YdbException(Status status) : base(status.ToString())
    {
        var policy = RetrySettings.DefaultInstance.GetRetryRule(status.StatusCode).Policy;

        IsTransient = policy == RetryPolicy.Unconditional;
        IsTransientWhenIdempotent = policy != RetryPolicy.None;
        // TODO: Add SQLSTATE message with order with https://en.wikipedia.org/wiki/SQLSTATE
    }

    public override bool IsTransient { get; }

    public bool IsTransientWhenIdempotent { get; }
}

public class YdbOperationInProgressException : DbException
{
    public YdbOperationInProgressException(YdbConnection ydbConnection)
        : base("A command is already in progress: " + ydbConnection.LastCommand)
    {
    }
}
