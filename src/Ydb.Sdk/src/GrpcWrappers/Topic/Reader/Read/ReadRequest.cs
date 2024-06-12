using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Read;

internal class ReadRequest
{
    public long BytesCount { get; set; }

    public StreamReadMessage.Types.ReadRequest ToProto()
    {
        return new StreamReadMessage.Types.ReadRequest
        {
            BytesSize = BytesCount
        };
    }
}