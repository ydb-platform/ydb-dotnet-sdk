using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic;

public class SendResult
{
    private readonly long _offset;

    internal SendResult(StreamWriteMessage.Types.WriteResponse.Types.WriteAck ack)
    {
        switch (ack.MessageWriteStatusCase)
        {
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Written:
                Status = PersistenceStatus.Written;
                _offset = ack.Written.Offset;
                break;
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Skipped:
                Status = PersistenceStatus.AlreadyWritten;
                break;
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.None:
            default:
                throw new YdbProducerException($"Unexpected WriteAck status: {ack.MessageWriteStatusCase}");
        }
    }

    public PersistenceStatus Status { get; }

    public bool TryGetOffset(out long offset)
    {
        offset = _offset;

        return Status == PersistenceStatus.Written;
    }
}

public enum PersistenceStatus
{
    Written,
    AlreadyWritten
}
