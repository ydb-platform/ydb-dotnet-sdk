using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal enum WriteStatusType
{
    Unknown = StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.None,
    Written = StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Written,
    Skipped = StreamWriteMessage.Types.WriteResponse.Types.WriteAck.MessageWriteStatusOneofCase.Skipped
}
