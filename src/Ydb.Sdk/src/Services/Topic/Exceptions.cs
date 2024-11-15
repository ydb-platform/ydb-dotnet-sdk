namespace Ydb.Sdk.Services.Topic;

public class YdbWriterException : Exception
{
    public YdbWriterException(string message) : base(message)
    {
        Status = new Status(StatusCode.Unspecified);
    }

    public YdbWriterException(string message, Status status) : base(message + ": " + status)
    {
        Status = status;
    }

    public YdbWriterException(string message, Driver.TransportException e) : base(message, e)
    {
        Status = e.Status;
    }

    public Status Status { get; }
}

public class YdbReaderException : Exception
{
    protected YdbReaderException(string message) : base(message)
    {
    }
}
