using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Writer;

public class WriteResult
{
    internal static readonly WriteResult Skipped = new();

    internal WriteResult(StreamWriteMessage.Types.WriteResponse.Types.WriteAck ack)
    {
        switch (ack.MessageWriteStatusCase)
        {
            case StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Written:
                Status = PersistenceStatus.Written;
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

    /// <summary>
    /// The persistence status of the message
    /// </summary>
    public PersistenceStatus Status { get; }
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
