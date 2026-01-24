using Ydb.Sdk.Coordinator.Description;
using Ydb.Sdk.Coordinator.Settings;

namespace Ydb.Sdk.Coordinator.Impl;

using Coordination;

public class ClientImpl : ICoordinationClient
{
    private readonly IRpc _rpc;

    public ClientImpl(IRpc rpc)
    {
        _rpc = rpc; // либо  rpc?? throw new ArgumentNullException(nameof(rpc));
    }

    private string ValidatePath(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            throw new ArgumentException("Coordination node path cannot be empty", nameof(path));
        }

        return path.StartsWith("/")
            ? path
            : $"{_rpc.Database}/{path}";
    }

    private static string GetTraceIdOrGenerateNew(string traceId)
        => string.IsNullOrEmpty(traceId)
            ? Guid.NewGuid().ToString()
            : traceId;

    // могут быть проблемы, связанные с withDeadline, так как его нет , аналог походу settings.TransportTimeout
    private static GrpcRequestSettings MakeGrpcRequestSettings(OperationSettings settings, string traceId)
        => new GrpcRequestSettings { TraceId = traceId , TransportTimeout = settings.TransportTimeout};
 

    /*
    @Override
    public CoordinationSession createSession(String path, CoordinationSessionSettings settings) {
        return new SessionImpl(rpc, Clock.systemUTC(), validatePath(path), settings);
    }
    */

    public Task CreateNode(string path, CoordinationNodeSettings settings)
    {
        var request = new CreateNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
            Config = settings.Config.ToProto()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId);
        return _rpc.CreateNodeAsync(request, grpcSettings);
    }

    public Task AlterNode(string path, CoordinationNodeSettings settings)
    {
        var request = new AlterNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
            Config = settings.Config.ToProto()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId);
        return _rpc.AlterNodeAsync(request, grpcSettings);
    }

    public Task DropNode(string path, DropCoordinationNodeSettings settings)
    {
        var request = new DropNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId);
        return _rpc.DropNodeAsync(request, grpcSettings);
    }

    public Task<NodeConfig> DescribeNode(String path,
        DescribeCoordinationNodeSettings settings)
    {
        var request = new DescribeNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
        };
        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId);
        return _rpc.DescribeNodeAsync(request, grpcSettings);
    }

    public string GetDatabase() => _rpc.Database;
}
