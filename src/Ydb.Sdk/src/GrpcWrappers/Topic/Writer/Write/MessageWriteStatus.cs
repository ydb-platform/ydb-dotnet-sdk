using static Ydb.Topic.StreamWriteMessage.Types.WriteResponse.Types.WriteAck;
using static Ydb.Topic.StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal class MessageWriteStatus
{
    public WriteStatusType Type { get; private set; }
    public long? WrittenOffset { get; private set; }
    public WriteStatusSkipReason SkippedReason { get; private set; }

    public static MessageWriteStatus FromProto(MessageWriteStatusOneofCase status, Written written, Skipped skipped)
    {
        var result = new MessageWriteStatus();
        switch (status)
        {
            case MessageWriteStatusOneofCase.Written:
                result.Type = WriteStatusType.Written;
                result.WrittenOffset = written.Offset;
                break;
            case MessageWriteStatusOneofCase.Skipped:
                result.Type = WriteStatusType.Skipped;
                result.SkippedReason = (WriteStatusSkipReason) skipped.Reason;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(status), status, $"Invalid status: {status}");
        }

        return result;
    }
}
