namespace Ydb.Sdk.Services.Topic;

public class WriterException : Exception
{
    public WriterException(string message) : base(message)
    {
    }

    public WriterException(string message, Status status) : base(message + ": " + status)
    {
    }

    public WriterException(string message, Exception inner) : base(message, inner)
    {
    }
}

public class ReaderException : Exception
{
    public ReaderException(string message) : base(message)
    {
        Status = new Status(StatusCode.Unspecified);
    }

    public ReaderException(string message, Status status) : base(message + ": " + status)
    {
        Status = status;
    }

    public ReaderException(string message, Driver.TransportException e) : base(message, e)
    {
        Status = e.Status;
    }

    public Status Status { get; }
}
