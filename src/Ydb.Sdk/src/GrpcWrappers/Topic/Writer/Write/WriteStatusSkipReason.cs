using static Ydb.Topic.StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Skipped.Types;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

public enum WriteStatusSkipReason
{
    Unspecified = Reason.Unspecified,
    AlreadyWritten = Reason.AlreadyWritten
}
