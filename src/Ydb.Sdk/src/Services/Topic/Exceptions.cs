namespace Ydb.Sdk.Services.Topic;

public class YdbTopicException : Exception
{
    protected YdbTopicException(string message) : base(message)
    {
    }
}

public class YdbWriterException : YdbTopicException
{
    public YdbWriterException(string message) : base(message)
    {
    }
}

public class YdbReaderException : YdbTopicException
{
    protected YdbReaderException(string message) : base(message)
    {
    }
}
