namespace Ydb.Sdk.GrpcWrappers.Topic.Exceptions;

internal class TopicWriterStoppedException: Exception
{
    public TopicWriterStoppedException()
    {
    }

    public TopicWriterStoppedException(string message) : base(message)
    {
    }
}
