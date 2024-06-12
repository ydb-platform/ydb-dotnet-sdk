using Ydb.Sdk.Client;
using static Ydb.Topic.StreamReadMessage.Types.FromServer;
using static Ydb.Topic.StreamReadMessage.Types;
using CommitOffsetResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.CommitOffset.CommitOffsetResponse;
using InitRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.Init.InitRequest;
using InitResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.Init.InitResponse;
using PartitionSessionStatusRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.PartitionSessionStatus.PartitionSessionStatusRequest;
using PartitionSessionStatusResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.PartitionSessionStatus.PartitionSessionStatusResponse;
using ReadResponse = Ydb.Sdk.GrpcWrappers.Topic.Reader.Read.ReadResponse;
using StartPartitionSessionRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.StartPartitionSession.StartPartitionSessionRequest;
using StopPartitionSessionRequest = Ydb.Sdk.GrpcWrappers.Topic.Reader.StopPartitionSession.StopPartitionSessionRequest;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader;

internal class ReadMessagesResponseStream: StreamResponse<FromServer, ITopicReaderResponse>
{
    public ReadMessagesResponseStream(Driver.StreamIterator<FromServer> iterator) : base(iterator)
    {
    }

    protected override ITopicReaderResponse MakeResponse(FromServer protoResponse)
    {
        return protoResponse.ServerMessageCase switch
        {
            ServerMessageOneofCase.InitResponse => InitResponse.FromProto(protoResponse),
            ServerMessageOneofCase.ReadResponse => ReadResponse.FromProto(protoResponse),
            ServerMessageOneofCase.CommitOffsetResponse => CommitOffsetResponse.FromProto(protoResponse),
            ServerMessageOneofCase.PartitionSessionStatusResponse =>
                PartitionSessionStatusResponse.FromProto(protoResponse),
            //ServerMessageOneofCase.UpdateTokenResponse => UpdateTokenResponse.FromProto(protoResponse),
            ServerMessageOneofCase.StartPartitionSessionRequest =>
                StartPartitionSessionRequest.FromProto(protoResponse),
            ServerMessageOneofCase.StopPartitionSessionRequest =>
                StopPartitionSessionRequest.FromProto(protoResponse),
            _ => throw new ArgumentOutOfRangeException(
                nameof(protoResponse),
                protoResponse.ServerMessageCase,
                "Unknown response type")
        };
    }

    protected override ITopicReaderResponse MakeResponse(Status status)
    {
        throw new NotImplementedException();
    }
}
