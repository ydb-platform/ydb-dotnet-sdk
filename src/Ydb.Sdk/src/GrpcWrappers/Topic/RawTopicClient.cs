using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;
using Ydb.Topic.V1;

namespace Ydb.Sdk.GrpcWrappers.Topic;

internal class RawTopicClient
{
    private readonly Driver _driver;

    public RawTopicClient(Driver driver)
    {
        _driver = driver;
    }

    public async Task<AlterTopicResponse> AlterTopic(AlterTopicRequest request)
    {
        var response = await _driver.UnaryCall(TopicService.AlterTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return AlterTopicResponse.FromProto(response);
    }

    public async Task<CreateTopicResponse> CreateTopic(CreateTopicRequest request)
    {
        var response = await _driver.UnaryCall(TopicService.CreateTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return CreateTopicResponse.FromProto(response);
    }

    public async Task<DescribeTopicResult> GetTopicDescription(DescribeTopicRequest request)
    {
        var response = await _driver.UnaryCall(TopicService.DescribeTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return DescribeTopicResult.FromProto(response);
    }

    public async Task<DropTopicResponse> DropTopic(DropTopicRequest request)
    {
        var response = await _driver.UnaryCall(TopicService.DropTopicMethod, request.ToProto(), new GrpcRequestSettings());
        return DropTopicResponse.FromProto(response);
    }
}
