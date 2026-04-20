using Microsoft.Extensions.Logging;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using static Ydb.Sdk.Ado.PoolManager;


namespace Ydb.Sdk.Coordination;

public class CoordinationClient
{
    private readonly IDriver _iDriver;
    private readonly ILogger<CoordinationClient> _logger;


    public CoordinationClient(string connectionString)
    {
        _iDriver = GetDriver(new YdbConnectionStringBuilder(connectionString)).AsTask().Result;
        _logger = _iDriver.LoggerFactory.CreateLogger<CoordinationClient>();
    }


    private string ValidatePath(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            throw new ArgumentException("Coordination node path cannot be empty", nameof(path));
        }

        return path.StartsWith('/')
            ? path
            : $"{_iDriver.Database}/{path}";
    }

    private static string GetTraceIdOrGenerateNew(string traceId)
        => string.IsNullOrEmpty(traceId)
            ? Guid.NewGuid().ToString()
            : traceId;

    private static GrpcRequestSettings MakeGrpcRequestSettings(OperationSettings settings, string traceId,
        CancellationToken cancellationToken)
        => new()
        {
            TraceId = traceId,
            TransportTimeout = settings.TransportTimeout,
            CancellationToken = cancellationToken
        };


    public CoordinationSession CreateSession(string pathNode, SessionOptions? sessionOptions = null,
        CancellationTokenSource? cancelTokenSource = null)
    {
        ValidatePath(pathNode);
        var options = (sessionOptions ?? SessionOptions.Default);
        return new CoordinationSession(_iDriver, pathNode, options, cancelTokenSource);
    }


    public async Task CreateNode(string path, CoordinationNodeSettings settings,
        CancellationToken cancellationToken = default)
    {
        var request = new CreateNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
            Config = settings.Config.ToProto()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, cancellationToken);
        try
        {
            Task task = _iDriver.UnaryCall(CoordinationService.CreateNodeMethod, request, grpcSettings);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Create node failed");
        }
    }

    public async Task AlterNode(string path, CoordinationNodeSettings settings,
        CancellationToken cancellationToken = default)
    {
        var request = new AlterNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
            Config = settings.Config.ToProto()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, cancellationToken);
        try
        {
            Task task = _iDriver.UnaryCall(CoordinationService.AlterNodeMethod, request, grpcSettings);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Alter node failed");
        }
    }

    public async Task DropNode(string path, DropCoordinationNodeSettings settings,
        CancellationToken cancellationToken = default)
    {
        var request = new DropNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, cancellationToken);
        try
        {
            Task task = _iDriver.UnaryCall(CoordinationService.DropNodeMethod, request, grpcSettings);
            await task;
        }

        catch (Exception)
        {
            throw new YdbException("Drop node failed");
        }
    }

    public async Task<NodeConfig> DescribeNode(string path,
        DescribeCoordinationNodeSettings settings, CancellationToken cancellationToken = default)
    {
        var request = new DescribeNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams()
        };
        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, cancellationToken);

        try
        {
            var task = _iDriver.UnaryCall(CoordinationService.DescribeNodeMethod, request, grpcSettings);
            await task;

            return NodeConfig.FromProto(task.Result.Operation.Result.Unpack<DescribeNodeResult>());
        }

        catch (Exception)
        {
            throw new YdbException("Describe node failed");
        }
    }
}
