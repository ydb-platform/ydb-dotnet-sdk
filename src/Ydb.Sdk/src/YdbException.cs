namespace Ydb.Sdk;

public class YdbException : Exception
{
    public YdbException(string message) : base(message)
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
