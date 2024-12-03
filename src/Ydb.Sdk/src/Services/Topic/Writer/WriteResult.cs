using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Writer;

public class WriteResult
{
    internal static readonly WriteResult Skipped = new();

    private readonly long _offset;

    internal WriteResult(StreamWriteMessage.Types.WriteResponse.Types.WriteAck ack)
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
                throw new WriterException($"Unexpected WriteAck status: {ack.MessageWriteStatusCase}");
        }
    }

    private WriteResult()
    {
        Status = PersistenceStatus.AlreadyWritten;
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
