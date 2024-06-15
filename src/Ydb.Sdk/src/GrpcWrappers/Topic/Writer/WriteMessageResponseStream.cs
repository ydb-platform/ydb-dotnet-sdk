using Ydb.Sdk.Client;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.UpdateToken;
using static Ydb.Topic.StreamWriteMessage.Types.FromServer;
using static Ydb.Topic.StreamWriteMessage.Types;
using InitResponse = Ydb.Sdk.GrpcWrappers.Topic.Writer.Init.InitResponse;
using WriteResponse = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.WriteResponse;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer;

internal class WriteMessageResponseStream: StreamResponse<FromServer, ITopicWriterResponse>
{
    public WriteMessageResponseStream(IAsyncEnumerator<FromServer> iterator) : base(iterator)
    {
    }

    protected override ITopicWriterResponse MakeResponse(FromServer protoResponse)
    {
        switch (protoResponse.ServerMessageCase)
        {
            case ServerMessageOneofCase.InitResponse:
                return InitResponse.FromProto(protoResponse);
            case ServerMessageOneofCase.WriteResponse:
                return WriteResponse.FromProto(protoResponse);
            case ServerMessageOneofCase.UpdateTokenResponse:
                return new UpdateTokenResponse();
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(protoResponse),
                    protoResponse.ServerMessageCase,
                    "Unknown response type");
        }
    }

    protected override ITopicWriterResponse MakeResponse(Status status)
    {
        throw new NotImplementedException();
    }
}
