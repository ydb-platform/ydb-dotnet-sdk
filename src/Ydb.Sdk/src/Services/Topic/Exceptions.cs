namespace Ydb.Sdk.Services.Topic;

public class WriterException : Exception
{
    public WriterException(string message) : base(message)
    {
        Status = new Status(StatusCode.Unspecified);
    }

    public WriterException(string message, Status status) : base(message + ": " + status)
    {
        Status = status;
    }

    public WriterException(string message, Driver.TransportException e) : base(message, e)
    {
        Status = e.Status;
    }

    public Status Status { get; }
}

public class ReaderException : Exception
{
    protected ReaderException(string message) : base(message)
    {
    }
}
