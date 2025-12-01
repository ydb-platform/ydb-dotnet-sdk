using Ydb.Topic;

namespace Ydb.Sdk.Topic.Writer;

public class WriteResult
{
    internal static readonly WriteResult Skipped = new();

    internal WriteResult(StreamWriteMessage.Types.WriteResponse.Types.WriteAck ack)
    {
        SeqNo = ack.SeqNo;

        switch (ack.MessageWriteStatusCase)
        {
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Written:
                Status = PersistenceStatus.Written;
                break;
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Skipped:
                Status = PersistenceStatus.AlreadyWritten;
                break;
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.WrittenInTx:
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.None:
            default:
                throw new WriterException($"Unexpected WriteAck status: {ack.MessageWriteStatusCase}");
        }
    }

    private WriteResult()
    {
        Status = PersistenceStatus.AlreadyWritten;
    }

    /// <summary>
    /// The persistence status of the message
    /// </summary>
    public PersistenceStatus Status { get; }

    /// <summary>
    /// SeqNo is a unique identifier within a specific ProducerId
    /// </summary>
    public long SeqNo { get; }
}

/// <summary>
/// Enumeration of possible message persistence states.
/// </summary>
public enum PersistenceStatus
{
    /// <summary>
    /// The message is recorded
    /// </summary>
    Written,

    /// <summary>
    /// The message was recorded in the last call session
    /// </summary>
    AlreadyWritten
}
