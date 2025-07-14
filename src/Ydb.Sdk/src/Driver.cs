using System.Collections.Immutable;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Discovery;
using Ydb.Discovery.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk;

public sealed class Driver : BaseDriver
{
    private const int AttemptDiscovery = 10;

    private readonly EndpointPool _endpointPool;

    internal string Database => Config.Database;

    public Driver(DriverConfig config, ILoggerFactory? loggerFactory = null)
        : base(config, loggerFactory ?? NullLoggerFactory.Instance,
            (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<Driver>()
        )
    {
        _endpointPool = new EndpointPool(LoggerFactory);
    }

    public static async Task<Driver> CreateInitialized(DriverConfig config, ILoggerFactory? loggerFactory = null)
    {
        var driver = new Driver(config, loggerFactory);
        await driver.Initialize();
        return driver;
    }

    public async Task Initialize()
    {
        Logger.LogInformation("Started initial endpoint discovery");

        for (var i = 0; i < AttemptDiscovery; i++)
        {
            try
            {
                await DiscoverEndpoints();

                _ = Task.Run(PeriodicDiscovery);

                return;
            }
            catch (YdbException e)
            {
                Logger.LogError(e, "Error during initial endpoint discovery: {e.Status}", e.Code);

                if (i == AttemptDiscovery - 1)
                {
                    throw;
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(i * 200)); // await 0 ms, 200 ms, 400ms, ... 1.8 sec
        }

        throw new YdbException("Error initial endpoint discovery");
    }

    protected override string GetEndpoint(long nodeId) => _endpointPool.GetEndpoint(nodeId);

    protected override void OnRpcError(string endpoint, RpcException e)
    {
        Logger.LogWarning("gRPC error [{Status}] on channel {Endpoint}", e.Status, endpoint);

        if (e.StatusCode is
            Grpc.Core.StatusCode.Cancelled or
            Grpc.Core.StatusCode.DeadlineExceeded or
            Grpc.Core.StatusCode.ResourceExhausted
           )
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

    private async Task DiscoverEndpoints()
    {
        using var channel = GrpcChannelFactory.CreateChannel(Config.Endpoint);

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

        var operation = response.Operation;
        if (operation.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(operation.Status, operation.Issues);
        }

        var resultProto = operation.Result.Unpack<ListEndpointsResult>();

        Logger.LogDebug(
            "Successfully discovered endpoints: {EndpointsCount}, self location: {SelfLocation}, sdk info: {SdkInfo}",
            resultProto.Endpoints.Count, resultProto.SelfLocation, Config.SdkVersion
        );

        await ChannelPool.RemoveChannels(
            _endpointPool.Reset(resultProto.Endpoints
                .Select(endpointSettings => new EndpointSettings(
                    (int)endpointSettings.NodeId,
                    (endpointSettings.Ssl ? "https://" : "http://") +
                    endpointSettings.Address + ":" + endpointSettings.Port,
                    endpointSettings.Location))
                .ToImmutableArray()
            )
        );
    }

    private async Task PeriodicDiscovery()
    {
        while (Disposed == 0)
        {
            try
            {
                await Task.Delay(Config.EndpointDiscoveryInterval);

                await DiscoverEndpoints();
            }
            catch (YdbException e)
            {
                Logger.LogWarning(e, "Error during endpoint discovery");
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Unexpected exception during session pool periodic check");
            }
        }
    }
}
