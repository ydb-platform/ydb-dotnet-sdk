using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordinator.Description;
using Ydb.Sdk.Coordinator.Settings;

namespace Ydb.Sdk.Coordinator.Impl;

public class CoordinationClient
{
    // IDriverFactory _driverFactory надо добавлять его через конструктор , а не IDriver?
    private readonly IDriver _iDriver;
    private readonly CancellationToken _cancellationToken;

    public CoordinationClient(IDriver iDriver, CancellationToken cancellationToken = default)
    {
        _iDriver = iDriver;
        _cancellationToken = cancellationToken;
    }


    private string ValidatePath(string path)
    {
        if (string.IsNullOrEmpty(path))
        {
            throw new ArgumentException("Coordination node path cannot be empty", nameof(path));
        }

        return path.StartsWith("/")
            ? path
            : $"{_iDriver.Database}/{path}";
    }

    private static string GetTraceIdOrGenerateNew(string traceId)
        => string.IsNullOrEmpty(traceId)
            ? Guid.NewGuid().ToString()
            : traceId;

    // могут быть проблемы, связанные с withDeadline, так как его нет , аналог походу settings.TransportTimeout
    private static GrpcRequestSettings MakeGrpcRequestSettings(OperationSettings settings, string traceId,
        CancellationToken cancellationToken)
        => new()
        {
            TraceId = traceId,
            TransportTimeout = settings.TransportTimeout,
            CancellationToken = cancellationToken
        };

    /*
    @Override
    public CoordinationSession createSession(String path, CoordinationSessionSettings settings) {
        return new SessionImpl(rpc, Clock.systemUTC(), validatePath(path), settings);
    }
    */

    public async Task CreateNode(string path, CoordinationNodeSettings settings)
    {
        var request = new CreateNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
            Config = settings.Config.ToProto()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, _cancellationToken);
        try
        {
            Task task = _iDriver.UnaryCall(CoordinationService.CreateNodeMethod, request, grpcSettings);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Create node failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Create node canceled");
            }
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception e)
        {
            throw new YdbException("Create node failed");
        }
    }

    public async Task AlterNode(string path, CoordinationNodeSettings settings)
    {
        var request = new AlterNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams(),
            Config = settings.Config.ToProto()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, _cancellationToken);
        try
        {
            Task task = _iDriver.UnaryCall(CoordinationService.AlterNodeMethod, request, grpcSettings);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Alter node failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Alter node canceled");
            }
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception e)
        {
            throw new YdbException("Alter node failed");
        }
    }

    public async Task DropNode(string path, DropCoordinationNodeSettings settings)
    {
        var request = new DropNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams()
        };

        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, _cancellationToken);
        try
        {
            Task task = _iDriver.UnaryCall(CoordinationService.DropNodeMethod, request, grpcSettings);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Drop node failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Drop node canceled");
            }
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception e)
        {
            throw new YdbException("Drop node failed");
        }
    }

    public async ValueTask<NodeConfig> DescribeNode(string path,
        DescribeCoordinationNodeSettings settings)
    {
        var request = new DescribeNodeRequest
        {
            Path = ValidatePath(path),
            OperationParams = settings.MakeOperationParams()
        };
        var traceId = GetTraceIdOrGenerateNew(settings.TraceId);
        var grpcSettings = MakeGrpcRequestSettings(settings, traceId, _cancellationToken);

        try
        {
            var task = _iDriver.UnaryCall(CoordinationService.DescribeNodeMethod, request, grpcSettings);
            await task;
            if (task.IsFaulted)
            {
                throw new YdbException("Describe node failed");
            }

            if (task.IsCanceled)
            {
                throw new YdbException("Describe node canceled");
            }

            return await new ValueTask<NodeConfig>(
                NodeConfig.FromProto(task.Result.Operation.Result.Unpack<DescribeNodeResult>()));
        }
        catch (YdbException e)
        {
            throw new YdbException(e.Message);
        }
        catch (Exception e)
        {
            throw new YdbException("Describe node failed");
        }
    }

    public string GetDatabase() => _iDriver.Database;
}
