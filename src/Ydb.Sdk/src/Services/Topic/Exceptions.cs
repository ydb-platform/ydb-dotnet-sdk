namespace Ydb.Sdk.Services.Topic;

public class WriterException : Exception
{
    public WriterException(string message) : base(message)
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
    }

    public ReaderException(string message, Exception inner) : base(message, inner)
    {
    }
}
