using Ydb.Sdk.GrpcWrappers.Topic.Extensions;
using Ydb.Topic;
using Codec = Ydb.Sdk.GrpcWrappers.Topic.Codecs.Codec;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal class WriteRequest
{
    public Codec Codec { get; set; }
    public List<Message> Messages { get; set; } = new();

    public StreamWriteMessage.Types.WriteRequest ToProto()
    {
        var result = new StreamWriteMessage.Types.WriteRequest
        {
            Codec = (int) Codec.ToProto()
        };
        result.Messages.AddRange(Messages.Select(m => m.ToProto()));

        return result;
    }
}
