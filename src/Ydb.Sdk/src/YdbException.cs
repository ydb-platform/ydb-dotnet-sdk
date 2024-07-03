namespace Ydb.Sdk;

public class YdbException : Exception
{
    public YdbException()
    {
    }

    public YdbException(string message) : base(message)
    {
    }

    public YdbException(Status status) : base(status.ToString())
    {
    }

    public YdbException(string message, Exception other) : base(message, other)
    {
    }
}
