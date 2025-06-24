using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbException : DbException
{
    internal YdbException(string message) : base(message)
    {
    }

    internal YdbException(Driver.TransportException transportException)
        : this(transportException.Status, transportException)
    {
    }

    internal YdbException(Status status, Exception? innerException = null)
        : this(status.StatusCode, status.ToString(), innerException)
    {
    }

    internal YdbException(StatusCode statusCode, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = statusCode;
        var policy = RetrySettings.DefaultInstance.GetRetryRule(statusCode).Policy;

        IsTransient = policy == RetryPolicy.Unconditional;
        IsTransientWhenIdempotent = policy != RetryPolicy.None;
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
