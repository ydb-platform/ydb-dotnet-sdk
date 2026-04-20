using Microsoft.Extensions.Logging;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Schema;
using Ydb.Sdk.Coordination.Description;
using static Ydb.Sdk.Ado.PoolManager;

namespace Ydb.Sdk.Coordination;

public class CoordinationClient
{
    private readonly string _connectionString;
    private readonly IDriver _iDriver;
    private readonly ILogger<CoordinationClient> _logger;


    public CoordinationClient(string connectionString)
    {
        _connectionString = connectionString;
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


    private static GrpcRequestSettings MakeGrpcRequestSettings(
        CancellationToken cancellationToken)
        => new()
        {
            CancellationToken = cancellationToken
        };


    public CoordinationSession CreateSession(string pathNode, SessionOptions? sessionOptions = null,
        CancellationTokenSource? cancelTokenSource = null)
    {
        _connectionString.FullPath(pathNode);
        var options = sessionOptions ?? SessionOptions.Default;
        return new CoordinationSession(_iDriver, pathNode, options, cancelTokenSource);
    }


    public async Task CreateNode(string path, NodeConfig config,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating coordination node at {Path}", path);
        var request = new CreateNodeRequest
        {
            Path = ValidatePath(path),
            Config = config.ToProto()
        };

        try
        {
            var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
            var response = await _iDriver.UnaryCall(CoordinationService.CreateNodeMethod, request, grpcSettings);
            Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Create node failed " + e.Message);
        }
    }

    public async Task AlterNode(string path, NodeConfig config,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Updating configuration of coordination node at {Path}", path);
        var request = new AlterNodeRequest
        {
            Path = ValidatePath(path),
            Config = config.ToProto()
        };

        try
        {
            var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
            var response = await _iDriver.UnaryCall(CoordinationService.AlterNodeMethod, request, grpcSettings);
            Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Alter node failed " + e.Message);
        }
    }

    public async Task DropNode(string path,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Dropping coordination node at {Path}", path);
        var request = new DropNodeRequest
        {
            Path = ValidatePath(path)
        };

        try
        {
            var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
            var response = await _iDriver.UnaryCall(CoordinationService.DropNodeMethod, request, grpcSettings);
            Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Drop node failed " + e.Message);
        }
    }

    public async Task<NodeConfig> DescribeNode(string path, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Reading configuration of coordination node at {Path}", path);
        var request = new DescribeNodeRequest
        {
            Path = ValidatePath(path)
        };

        try
        {
            var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
            var response = await _iDriver.UnaryCall(CoordinationService.DescribeNodeMethod, request, grpcSettings);
            Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
            return NodeConfig.FromProto(response.Operation.Result.Unpack<DescribeNodeResult>());
        }
        catch (Exception e)
        {
            throw new YdbException("Describe node failed " + e.Message);
        }
    }
}

/*
 * public class CoordinationClient
{
    private readonly string _connectionString;
    private readonly IDriver _iDriver;
    private readonly ILogger<CoordinationClient> _logger;


    public CoordinationClient(string connectionString)
    {
        _connectionString = connectionString;
        _iDriver = GetDriver(new YdbConnectionStringBuilder(connectionString)).AsTask().Result;
        _logger = _iDriver.LoggerFactory.CreateLogger<CoordinationClient>();
    }

    private static GrpcRequestSettings MakeGrpcRequestSettings(
        CancellationToken cancellationToken)
        => new()
        {
            CancellationToken = cancellationToken
        };


    public CoordinationSession CreateSession(string pathNode, SessionOptions? sessionOptions = null,
        CancellationTokenSource? cancelTokenSource = null)
    {
        _connectionString.FullPath(pathNode);
        var options = sessionOptions ?? SessionOptions.Default;
        return new CoordinationSession(_iDriver, pathNode, options, cancelTokenSource);
    }


    public async Task CreateNode(string path, NodeConfig config,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating coordination node at {Path}", path);
        var request = new CreateNodeRequest
        {
            Path = _connectionString.FullPath(path),
            Config = config.ToProto()
        };

        var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
        var response = await _iDriver.UnaryCall(CoordinationService.CreateNodeMethod, request, grpcSettings);
        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
    }

    public async Task AlterNode(string path, NodeConfig config,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Updating configuration of coordination node at {Path}", path);
        var request = new AlterNodeRequest
        {
            Path = _connectionString.FullPath(path),
            Config = config.ToProto()
        };

        var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
        var response = await _iDriver.UnaryCall(CoordinationService.AlterNodeMethod, request, grpcSettings);
        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
    }

    public async Task DropNode(string path,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Dropping coordination node at {Path}", path);
        var request = new DropNodeRequest
        {
            Path = _connectionString.FullPath(path),
        };

        var grpcSettings = MakeGrpcRequestSettings(cancellationToken);
        var response = await _iDriver.UnaryCall(CoordinationService.DropNodeMethod, request, grpcSettings);
        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
    }

    public async Task<NodeConfig> DescribeNode(string path, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Reading configuration of coordination node at {Path}", path);
        var request = new DescribeNodeRequest
        {
            Path = _connectionString.FullPath(path),
        };

        var grpcSettings = MakeGrpcRequestSettings(cancellationToken);

        var response = await _iDriver.UnaryCall(CoordinationService.DescribeNodeMethod, request, grpcSettings);
        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
        return NodeConfig.FromProto(response.Operation.Result.Unpack<DescribeNodeResult>());
    }
}
 */
