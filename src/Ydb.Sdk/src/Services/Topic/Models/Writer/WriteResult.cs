using Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;
using Ydb.Sdk.Utils;

namespace Ydb.Sdk.Services.Topic.Models.Writer;

public class WriteResult
{
    public long? Offset { get; private set; }
    public WriteResultStatus Status { get; private set; }

    internal static WriteResult FromWrapper(MessageWriteStatus writeResult)
    {
        return new WriteResult
        {
            Offset = writeResult.WrittenOffset,
            Status = EnumConverter.Convert<WriteStatusType, WriteResultStatus>(writeResult.Type)
        };
    }
}
