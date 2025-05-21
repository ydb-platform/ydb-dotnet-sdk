using System.Collections.Immutable;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Discovery;
using Ydb.Discovery.V1;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Services.Auth;

namespace Ydb.Sdk;

public sealed class Driver : BaseDriver
{
    private const int AttemptDiscovery = 10;

    private readonly GrpcChannelFactory _grpcChannelFactory;
    private readonly EndpointPool _endpointPool;
    private readonly ChannelPool<GrpcChannel> _channelPool;

    internal string Database => Config.Database;

    public Driver(DriverConfig config, ILoggerFactory? loggerFactory = null)
        : base(config, loggerFactory ?? NullLoggerFactory.Instance,
            (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<Driver>()
        )
    {
        _grpcChannelFactory = new GrpcChannelFactory(LoggerFactory, config);
        _endpointPool = new EndpointPool(LoggerFactory.CreateLogger<EndpointPool>());
        _channelPool = new ChannelPool<GrpcChannel>(
            LoggerFactory.CreateLogger<ChannelPool<GrpcChannel>>(),
            _grpcChannelFactory
        );

        CredentialsProvider = Config.User != null
            ? new CachedCredentialsProvider(
                new StaticCredentialsAuthClient(config, _grpcChannelFactory, LoggerFactory),
                LoggerFactory
            )
            : Config.Credentials;
    }

    public static async Task<Driver> CreateInitialized(DriverConfig config, ILoggerFactory? loggerFactory = null)
    {
        var driver = new Driver(config, loggerFactory);
        await driver.Initialize();
        return driver;
    }

    protected override ValueTask InternalDispose() => _channelPool.DisposeAsync();

    public async Task Initialize()
    {
        Logger.LogInformation("Started initial endpoint discovery");

        for (var i = 0; i < AttemptDiscovery; i++)
        {
            try
            {
                var status = await DiscoverEndpoints();
                if (status.IsSuccess)
                {
                    _ = Task.Run(PeriodicDiscovery);

                    return;
                }

                Logger.LogCritical("Error during initial endpoint discovery: {status}", status);
            }
            catch (RpcException e)
            {
                Logger.LogCritical("RPC error during initial endpoint discovery: {e.Status}", e.Status);

                if (i == AttemptDiscovery - 1)
                {
                    throw;
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(i * 200)); // await 0 ms, 200 ms, 400ms, .. 1.8 sec
        }

        throw new InitializationFailureException("Error during initial endpoint discovery");
    }

    protected override (string, GrpcChannel) GetChannel(long nodeId)
    {
        var endpoint = _endpointPool.GetEndpoint(nodeId);

        return (endpoint, _channelPool.GetChannel(endpoint));
    }

    protected override void OnRpcError(string endpoint, RpcException e)
    {
        Logger.LogWarning("gRPC error [{Status}] on channel {Endpoint}", e.Status, endpoint);

        if (e.StatusCode is Grpc.Core.StatusCode.Cancelled or Grpc.Core.StatusCode.DeadlineExceeded)
        {
            return;
        }

        if (!_endpointPool.PessimizeEndpoint(endpoint))
        {
            return;
        }

        Logger.LogInformation("Too many pessimized endpoints, initiated endpoint rediscovery.");

        _ = Task.Run(DiscoverEndpoints);
    }

    protected override ICredentialsProvider? CredentialsProvider { get; }

    private async Task<Status> DiscoverEndpoints()
    {
        using var channel = _grpcChannelFactory.CreateChannel(Config.Endpoint);

        var client = new DiscoveryService.DiscoveryServiceClient(channel);

        var request = new ListEndpointsRequest
        {
            Database = Config.Database
        };

        var requestSettings = new GrpcRequestSettings
        {
            TransportTimeout = Config.EndpointDiscoveryTimeout
        };

        var response = await client.ListEndpointsAsync(
            request: request,
            options: await GetCallOptions(requestSettings)
        );

        if (!response.Operation.Ready)
        {
            const string error = "Unexpected non-ready endpoint discovery operation.";
            Logger.LogError($"Endpoint discovery internal error: {error}");

            return new Status(StatusCode.ClientInternalError, error);
        }

        var status = Status.FromProto(response.Operation.Status, response.Operation.Issues);
        if (status.IsNotSuccess)
        {
            Logger.LogWarning("Unsuccessful endpoint discovery: {Status}", status);
            return status;
        }

        if (response.Operation.Result is null)
        {
            const string error = "Unexpected empty endpoint discovery result.";
            Logger.LogError($"Endpoint discovery internal error: {error}");

            return new Status(StatusCode.ClientInternalError, error);
        }

        var resultProto = response.Operation.Result.Unpack<ListEndpointsResult>();

        Logger.LogDebug(
            "Successfully discovered endpoints: {EndpointsCount}, self location: {SelfLocation}, sdk info: {SdkInfo}",
            resultProto.Endpoints.Count, resultProto.SelfLocation, Config.SdkVersion
        );

        await _channelPool.RemoveChannels(
            _endpointPool.Reset(resultProto.Endpoints
                .Select(endpointSettings => new EndpointSettings(
                    (int)endpointSettings.NodeId,
                    (endpointSettings.Ssl ? "https://" : "http://") +
                    endpointSettings.Address + ":" + endpointSettings.Port,
                    endpointSettings.Location))
                .ToImmutableArray()
            )
        );

        return new Status(StatusCode.Success);
    }

    private async Task PeriodicDiscovery()
    {
        while (Disposed == 0)
        {
            try
            {
                await Task.Delay(Config.EndpointDiscoveryInterval);

                _ = await DiscoverEndpoints();
            }
            catch (RpcException e)
            {
                Logger.LogWarning("RPC error during endpoint discovery: {Status}", e.Status);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Unexpected exception during session pool periodic check");
            }
        }
    }

    public class InitializationFailureException : Exception
    {
        internal InitializationFailureException(string message) : base(message)
        {
        }
    }

    public class TransportException : IOException
    {
        internal TransportException(RpcException e) : base($"Transport exception: {e.Message}", e)
        {
            Status = e.Status.ConvertStatus();
        }

        public Status Status { get; }
    }
}
