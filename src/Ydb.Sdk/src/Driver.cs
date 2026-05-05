using System.Collections.Immutable;
using System.Diagnostics;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Discovery;
using Ydb.Discovery.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Tracing;
using EndpointInfo = Ydb.Sdk.Pool.EndpointInfo;

namespace Ydb.Sdk;

/// <summary>
/// Main YDB driver implementation that handles endpoint discovery and connection management.
/// </summary>
/// <remarks>
/// The Driver class provides the primary interface for connecting to YDB clusters.
/// It automatically discovers available endpoints and manages gRPC connections.
/// </remarks>
public sealed class Driver : BaseDriver
{
    private static readonly YdbRetryPolicyExecutor DiscoveryRetryPolicy = new(
        new YdbRetryPolicy(new YdbRetryPolicyConfig { EnableRetryIdempotence = true })
    );

    private readonly EndpointPool _endpointPool;
    private readonly EndpointLocalDcDetector _endpointLocalDcDetector;

    private volatile Timer? _discoveryTimer;

    /// <summary>
    /// Initializes a new instance of the Driver class.
    /// </summary>
    /// <param name="config">Driver configuration settings.</param>
    /// <param name="loggerFactory">Optional logger factory for logging. If null, NullLoggerFactory will be used.</param>
    public Driver(DriverConfig config, ILoggerFactory? loggerFactory = null) :
        base(config, loggerFactory ?? NullLoggerFactory.Instance,
            (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<Driver>())
    {
        _endpointPool = new EndpointPool(LoggerFactory);
        _endpointLocalDcDetector = new EndpointLocalDcDetector(LoggerFactory);
    }

    /// <summary>
    /// Creates and initializes a new Driver instance in one operation.
    /// </summary>
    /// <param name="config">Driver configuration settings.</param>
    /// <param name="loggerFactory">Optional logger factory for logging. If null, NullLoggerFactory will be used.</param>
    /// <returns>A fully initialized Driver instance ready for use.</returns>
    /// <exception cref="YdbException">Thrown when endpoint discovery fails after all retry attempts.</exception>
    public static async Task<Driver> CreateInitialized(DriverConfig config, ILoggerFactory? loggerFactory = null)
    {
        var driver = new Driver(config, loggerFactory);
        await driver.Initialize();
        return driver;
    }

    /// <summary>
    /// Initializes the driver by discovering available endpoints.
    /// </summary>
    /// <remarks>
    /// This method performs initial endpoint discovery and starts periodic discovery.
    /// It will retry up to 10 times with exponential backoff if discovery fails.
    /// </remarks>
    /// <exception cref="YdbException">Thrown when endpoint discovery fails after all retry attempts.</exception>
    public async Task Initialize()
    {
        Logger.LogInformation("Started initial endpoint discovery");

        using var dbActivity = YdbActivitySource.StartActivity("ydb.Driver.Initialize", ActivityKind.Internal);
        try
        {
            await DiscoveryRetryPolicy.ExecuteAsync(async _ =>
            {
                await DiscoverEndpoints();
                _discoveryTimer = new Timer(
                    OnDiscoveryTimer,
                    null,
                    Config.EndpointDiscoveryInterval,
                    Config.EndpointDiscoveryInterval
                );
            });
        }
        catch (YdbException e)
        {
            dbActivity?.SetException(e);
            throw;
        }
    }

    private async void OnDiscoveryTimer(object? state)
    {
        try
        {
            await DiscoverEndpoints();
        }
        catch (YdbException e)
        {
            Logger.LogWarning(e, "Error during endpoint discovery");
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Unexpected exception during grpc channel pool periodic check");
        }
    }

    protected override EndpointInfo GetEndpoint(long nodeId) => _endpointPool.GetEndpoint(nodeId);

    protected override void OnRpcError(EndpointInfo endpointInfo, RpcException e)
    {
        Logger.LogWarning("gRPC error [{Status}] on channel {Endpoint}", e.Status, endpointInfo.Endpoint);

        if (e.StatusCode is
            Grpc.Core.StatusCode.Cancelled or
            Grpc.Core.StatusCode.DeadlineExceeded or
            Grpc.Core.StatusCode.ResourceExhausted
           )
        {
            return;
        }

        if (!_endpointPool.PessimizeEndpoint(endpointInfo))
        {
            return;
        }

        ScheduleRediscoveryIfDegraded();
    }

    /// <inheritdoc />
    public override void PessimizeNode(long nodeId)
    {
        if (!_endpointPool.PessimizeByNodeId(nodeId))
        {
            return;
        }

        ScheduleRediscoveryIfDegraded();
    }

    private void ScheduleRediscoveryIfDegraded()
    {
        Logger.LogInformation("Too many pessimized endpoints, initiated endpoint rediscovery.");

        // Reset timer to trigger discovery sooner, ensuring single-threaded execution through timer callback
        _discoveryTimer?.Change(TimeSpan.Zero, Config.EndpointDiscoveryInterval);
    }

    /// <summary>
    /// Disposes the driver and stops periodic endpoint discovery.
    /// </summary>
    protected override async ValueTask DisposeAsyncCore()
    {
        if (_discoveryTimer != null)
        {
            await _discoveryTimer.DisposeAsync();
        }
    }

    private async Task DiscoverEndpoints()
    {
        try
        {
            using var channel = GrpcChannelFactory.CreateChannel(Config.Endpoint);

            var client = new DiscoveryService.DiscoveryServiceClient(channel);

            var request = new ListEndpointsRequest { Database = Config.Database };
            var grpcSettings = new GrpcRequestSettings { TransportTimeout = Config.EndpointDiscoveryTimeout };

            var response = await client.ListEndpointsAsync(
                request: request,
                options: await GetCallOptions(grpcSettings, Config.EndpointInfo)
            );

            var operation = response.Operation;
            if (operation.Status.IsNotSuccess())
            {
                throw YdbException.FromServer(operation.Status, operation.Issues);
            }

            var resultProto = operation.Result.Unpack<ListEndpointsResult>();
            var discoveredEndpoints = resultProto.Endpoints.Select(infoProto =>
                new EndpointInfo(infoProto.NodeId, infoProto.Ssl, infoProto.Address, infoProto.Port, infoProto.Location)
            ).ToImmutableList();

            string? preferredLocation = null;
            if (Config.EnablePreferNearestDcBalancing)
            {
                try
                {
                    var detectedLocation = await _endpointLocalDcDetector.DetectNearestLocationDc(
                        discoveredEndpoints,
                        Config.ConnectTimeout
                    );

                    if (!string.IsNullOrEmpty(detectedLocation))
                    {
                        preferredLocation = detectedLocation;
                        Logger.LogInformation(
                            "Detected nearest DC via TCP latency: {DetectedLocation} (server reported: {SelfLocation})",
                            detectedLocation,
                            resultProto.SelfLocation
                        );
                    }
                    else
                    {
                        Logger.LogWarning(
                            "Failed to detect nearest DC via TCP latency, no preferred location will be used"
                        );
                    }
                }
                catch (Exception e)
                {
                    Logger.LogWarning(
                        e,
                        "Failed to detect nearest DC via TCP latency, no preferred location will be used"
                    );
                }
            }

            Logger.LogDebug(
                "Successfully discovered endpoints: {EndpointsCount}, self location: {SelfLocation}, preferred location: {PreferredLocation}, sdk info: {SdkInfo}",
                resultProto.Endpoints.Count,
                resultProto.SelfLocation,
                preferredLocation ?? "<disabled>",
                Config.SdkVersion
            );

            await ChannelPool.RemoveChannels(_endpointPool.Reset(
                discoveredEndpoints,
                preferredLocation
            ));
        }
        catch (RpcException e)
        {
            throw new YdbException(e);
        }
    }
}
