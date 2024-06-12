using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;
using Ydb.Topic.V1;
using StreamWriter = Ydb.Sdk.GrpcWrappers.Topic.StreamWriter;

namespace Ydb.Sdk.GrpcWrappers.Topic;

internal class RawTopicClient
{
    private readonly Driver driver;

    public RawTopicClient(Driver driver)
    {
        this.driver = driver;
    }

    public async Task<AlterTopicResponse> AlterTopic(AlterTopicRequest request)
    {
        var response = await driver.UnaryCall(TopicService.AlterTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return AlterTopicResponse.FromProto(response);
    }

    public async Task<CreateTopicResponse> CreateTopic(CreateTopicRequest request)
    {
        var response = await driver.UnaryCall(TopicService.CreateTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return CreateTopicResponse.FromProto(response);
    }

    public async Task<DescribeTopicResponse> DescribeTopic(DescribeTopicRequest request)
    {
        var response = await driver.UnaryCall(TopicService.DescribeTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return DescribeTopicResponse.FromProto(response);
    }

    public async Task<DropTopicResponse> DropTopic(DropTopicRequest request)
    {
        var response = await driver.UnaryCall(TopicService.DropTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return DropTopicResponse.FromProto(response);
    }

    public StreamWriter GetStreamWriter()
    {
        throw new NotImplementedException();
    }

    public StreamReader GetStreamReader()
    {
        throw new NotImplementedException();
    }
}
