namespace Ydb.Sdk.Services.Topic;

public class YdbTopicException : Exception
{
    protected YdbTopicException(string message) : base(message)
    {
    }
}

public class YdbProducerException : YdbTopicException
{
    public YdbProducerException(string message) : base(message)
    {
    }
}
