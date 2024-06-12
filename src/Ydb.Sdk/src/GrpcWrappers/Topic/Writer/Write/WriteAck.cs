using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal class WriteAck
{
    public long SequenceNumber { get; private set; }
    public MessageWriteStatus WriteStatus { get; private set; } = null!;

    public static WriteAck FromProto(StreamWriteMessage.Types.WriteResponse.Types.WriteAck writeAck)
    {
        return new WriteAck
        {
            SequenceNumber = writeAck.SeqNo,
            WriteStatus = MessageWriteStatus.FromProto(
                writeAck.MessageWriteStatusCase,
                writeAck.Written,
                writeAck.Skipped)
        };
    }
}
