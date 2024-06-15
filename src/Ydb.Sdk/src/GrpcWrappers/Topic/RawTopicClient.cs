namespace Ydb.Sdk.GrpcWrappers.Topic;

internal class RawTopicClient
{
    private readonly Driver driver;

    public RawTopicClient(Driver driver)
    {
        this.driver = driver;
    }

    public StreamWriter GetStreamWriter()
    {
        throw new NotImplementedException();
    }
}
