using Ydb.Coordination;
using Ydb.Sdk.Coordinator.Description;

namespace Ydb.Sdk.Coordinator.Impl;

public interface IRpc
{
    // GrpcReadWriteStream<SessionResponse, SessionRequest> CreateSession(GrpcRequestSettings settings);

    Task CreateNodeAsync(CreateNodeRequest request, GrpcRequestSettings settings);

    Task AlterNodeAsync(AlterNodeRequest request, GrpcRequestSettings settings);

    Task DropNodeAsync(DropNodeRequest request, GrpcRequestSettings settings);

    Task<NodeConfig> DescribeNodeAsync(DescribeNodeRequest request, GrpcRequestSettings settings);

    string Database { get; } // проблемы мб

    TaskScheduler Scheduler { get; } // возможно будут проблемы,изучить вопрос
}
