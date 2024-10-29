using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Reader;

public class Message<TValue>
{
    internal Message(TValue data, string topic, string producerId)
    {
        Data = data;
        Topic = topic;
        ProducerId = producerId;
    }

    public TValue Data { get; }

    /// <summary>
    /// The topic associated with the message.
    /// </summary>
    public string Topic { get; }

    public string ProducerId { get; }

    public Task Commit()
    {
        throw new NotImplementedException();
    }
}

public class BatchMessage<TValue>
{
    public BatchMessage(IReadOnlyCollection<Message<TValue>> batch)
    {
        Batch = batch;
    }

    public IReadOnlyCollection<Message<TValue>> Batch { get; }

    public Task Commit()
    {
        throw new NotImplementedException();
    }
}
