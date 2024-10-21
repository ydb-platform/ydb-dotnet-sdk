// using System.Collections.Concurrent;
// using Microsoft.Extensions.Logging;
// using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic;

// using ProducerStream = Driver.BidirectionalStream<
//     StreamWriteMessage.Types.FromClient,
//     StreamWriteMessage.Types.FromServer
// >;

internal class Producer<TValue> : IProducer<TValue>
{
    // private readonly Driver _driver;
    // private readonly ILogger<Producer<TValue>> _logger;
    // private readonly long _partitionId;
    // private readonly string _sessionId;
    // private readonly ISerializer<TValue> _serializer;
    //
    // private long _seqNum;
    //
    // private readonly ConcurrentQueue<StreamWriteMessage.Types.FromClient> _inFlightMessages;
    // private volatile ProducerStream _stream;
    //
    // internal Producer(
    //     ProducerConfig producerConfig,
    //     StreamWriteMessage.Types.InitResponse initResponse,
    //     ProducerStream stream,
    //     ISerializer<TValue> serializer)
    // {
    //     _driver = producerConfig.Driver;
    //     _stream = stream;
    //     _serializer = serializer;
    //     _logger = producerConfig.Driver.LoggerFactory.CreateLogger<Producer<TValue>>();
    //     _partitionId = initResponse.PartitionId;
    //     _sessionId = initResponse.SessionId;
    //     _seqNum = initResponse.LastSeqNo;
    //     _inFlightMessages = new ConcurrentQueue<StreamWriteMessage.Types.FromClient>();
    // }

    public Task<SendResult> SendAsync(TValue data)
    {
        throw new NotImplementedException();
    }

    public Task<SendResult> SendAsync(Message<TValue> message)
    {
        throw new NotImplementedException();
    }
}

public class Message<TValue>
{
    public Message(TValue data)
    {
        Data = data;
    }

    public DateTime Timestamp { get; set; }

    public TValue Data { get; }

    public List<Metadata> Metadata { get; } = new();
}

public record Metadata(string Key, byte[] Value);

public class SendResult
{
    public SendResult(State status)
    {
        State = status;
    }

    public State State { get; }
}

public enum State
{
    Written,
    AlreadyWritten
}

internal enum ProducerState
{
    Ready
    // Broken 
}
