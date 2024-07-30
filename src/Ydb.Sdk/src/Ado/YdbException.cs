namespace Ydb.Sdk.Ado;

public class YdbException : Exception
{
    public YdbException(string message) : base(message)
    {
    }

    public YdbException(string message, Exception e) : base(message, e)
    {
    }

    public YdbException(Status status) : base(status.ToString())
    {
    }
}

public class YdbOperationInProgressException : Exception
{
    public YdbOperationInProgressException(YdbConnection ydbConnection)
        : base("A command is already in progress: " + ydbConnection.LastCommand)
    {
    }
}
