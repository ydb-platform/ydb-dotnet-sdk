using System.Reflection;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Discovery;
using Ydb.Discovery.V1;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk;

public class Driver : GrpcTransport
{
    private readonly string _sdkInfo;
    private readonly ChannelsCache _channels;

    internal string Database => Config.Database;

    public Driver(DriverConfig config, ILoggerFactory? loggerFactory = null) : base(config, loggerFactory)
    {
        _channels = new ChannelsCache(Config, LoggerFactory);

        var version = Assembly.GetExecutingAssembly().GetName().Version;
        var versionStr = version is null ? "unknown" : version.ToString(3);
        _sdkInfo = $"ydb-dotnet-sdk/{versionStr}";
    }

    public static async Task<Driver> CreateInitialized(DriverConfig config, ILoggerFactory? loggerFactory = null)
    {
        var driver = new Driver(config, loggerFactory);
        await driver.Initialize();
        return driver;
    }

    protected override void Dispose(bool disposing)
    {
        Disposed = true;

        if (disposing)
        {
            _channels.Dispose();
        }
    }

    public async Task Initialize()
    {
        if (Config.Credentials is IUseDriverConfig useDriverConfig)
        {
            await useDriverConfig.ProvideConfig(Config);
            Logger.LogInformation("DriverConfig provided to IUseDriverConfig interface");
        }

        Logger.LogInformation("Started initial endpoint discovery");

        for (var i = 0; i < Config.AttemptDiscovery; i++)
        {
            try
            {
                var status = await DiscoverEndpoints();
                if (status.IsSuccess)
                {
                    _ = Task.Run(PeriodicDiscovery);

                    return;
                }

                Logger.LogCritical($"Error during initial endpoint discovery: {status}");
            }
            catch (RpcException e)
            {
                Logger.LogCritical($"RPC error during initial endpoint discovery: {e.Status}");

                if (i == Config.AttemptDiscovery - 1)
                {
                    throw;
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(i * 200)); // await 0 ms, 200 ms, 400ms, .. 1.8 sec
        }

        throw new InitializationFailureException("Error during initial endpoint discovery");
    }

    internal StreamIterator<TResponse> StreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        RequestSettings settings,
        string? preferredEndpoint = null)
        where TRequest : class
        where TResponse : class
    {
        var (endpoint, channel) = _channels.GetChannel(preferredEndpoint);
        var callInvoker = channel.CreateCallInvoker();

        var call = callInvoker.AsyncServerStreamingCall(
            method: method,
            host: null,
            options: GetCallOptions(settings, true),
            request: request);

        return new StreamIterator<TResponse>(
            call,
            _ => { OnRpcError(endpoint); });
    }

    private async Task<Status> DiscoverEndpoints()
    {
        using var channel = ChannelsCache.CreateChannel(Config.Endpoint, Config, LoggerFactory);

        var client = new DiscoveryService.DiscoveryServiceClient(channel);

        var request = new ListEndpointsRequest
        {
            Database = Config.Database
        };

        var requestSettings = new RequestSettings
        {
            TransportTimeout = Config.EndpointDiscoveryTimeout
        };

        var options = GetCallOptions(requestSettings, false);
        options.Headers.Add(Metadata.RpcSdkInfoHeader, _sdkInfo);

        var response = await client.ListEndpointsAsync(
            request: request,
            options: options);

        if (!response.Operation.Ready)
        {
            var error = "Unexpected non-ready endpoint discovery operation.";
            Logger.LogError($"Endpoint discovery internal error: {error}");

            return new Status(StatusCode.ClientInternalError, error);
        }

        var status = Status.FromProto(response.Operation.Status, response.Operation.Issues);
        if (!status.IsSuccess)
        {
            Logger.LogWarning($"Unsuccessful endpoint discovery: {status}");
            return status;
        }

        if (response.Operation.Result is null)
        {
            var error = "Unexpected empty endpoint discovery result.";
            Logger.LogError($"Endpoint discovery internal error: {error}");

            return new Status(StatusCode.ClientInternalError, error);
        }

        var resultProto = response.Operation.Result.Unpack<ListEndpointsResult>();

        Logger.LogInformation($"Successfully discovered endpoints: {resultProto.Endpoints.Count}" +
                              $", self location: {resultProto.SelfLocation}" +
                              $", sdk info: {_sdkInfo}");

        _channels.UpdateEndpoints(resultProto);

        return new Status(StatusCode.Success);
    }

    private async Task PeriodicDiscovery()
    {
        while (!Disposed)
        {
            try
            {
                await Task.Delay(Config.EndpointDiscoveryInterval);
                _ = await DiscoverEndpoints();
            }
            catch (RpcException e)
            {
                Logger.LogWarning($"RPC error during endpoint discovery: {e.Status}");
            }
            catch (Exception e)
            {
                Logger.LogError($"Unexpected exception during session pool periodic check: {e}");
            }
        }
    }

    protected override void OnRpcError(string endpoint)
    {
        var ratio = _channels.PessimizeEndpoint(endpoint);
        if (ratio != null && ratio > Config.PessimizedEndpointRatioTreshold)
        {
            Logger.LogInformation("Too many pessimized endpoints, initiated endpoint rediscovery.");
            _ = Task.Run(DiscoverEndpoints);
        }
    }

    protected override (string, GrpcChannel) GetChannel(string? preferredEndpoint)
    {
        return _channels.GetChannel(preferredEndpoint);
    }

    private CallOptions GetCallOptions(RequestSettings settings, bool streaming)
    {
        var meta = new Grpc.Core.Metadata
        {
            { Metadata.RpcDatabaseHeader, Config.Database }
        };

        var authInfo = Config.Credentials.GetAuthInfo();
        if (authInfo != null)
        {
            meta.Add(Metadata.RpcAuthHeader, authInfo);
        }

        if (settings.TraceId.Length > 0)
        {
            meta.Add(Metadata.RpcTraceIdHeader, settings.TraceId);
        }

        var transportTimeout = streaming
            ? Config.DefaultStreamingTransportTimeout
            : Config.DefaultTransportTimeout;

        if (settings.TransportTimeout != null)
        {
            transportTimeout = settings.TransportTimeout.Value;
        }

        var options = new CallOptions(
            headers: meta
        );

        if (transportTimeout != TimeSpan.Zero)
        {
            options = options.WithDeadline(DateTime.UtcNow + transportTimeout);
        }

        return options;
    }

    private static Status ConvertStatus(Grpc.Core.Status rpcStatus)
    {
        return new Status(
            rpcStatus.StatusCode switch
            {
                Grpc.Core.StatusCode.Unavailable => StatusCode.ClientTransportUnavailable,
                Grpc.Core.StatusCode.DeadlineExceeded => StatusCode.ClientTransportTimeout,
                Grpc.Core.StatusCode.ResourceExhausted => StatusCode.ClientTransportResourceExhausted,
                Grpc.Core.StatusCode.Unimplemented => StatusCode.ClientTransportUnimplemented,
                _ => StatusCode.ClientTransportUnknown
            },
            new List<Issue> { new(rpcStatus.Detail) }
        );
    }

    // TODO Refactoring
    internal sealed class UnaryResponse<TResponse>
    {
        internal UnaryResponse(TResponse data,
            string usedEndpoint,
            Grpc.Core.Metadata? trailers)
        {
            Data = data;
            UsedEndpoint = usedEndpoint;
            Trailers = trailers;
        }

        public TResponse Data { get; }

        public string UsedEndpoint { get; }

        public Grpc.Core.Metadata? Trailers { get; }
    }

    internal sealed class StreamIterator<TResponse>
    {
        private readonly AsyncServerStreamingCall<TResponse> _responseStream;
        private readonly Action<RpcException> _rpcErrorAction;

        internal StreamIterator(AsyncServerStreamingCall<TResponse> responseStream, Action<RpcException> rpcErrorAction)
        {
            _responseStream = responseStream;
            _rpcErrorAction = rpcErrorAction;
        }

        public TResponse Response => _responseStream.ResponseStream.Current;

        public async Task<bool> Next()
        {
            try
            {
                return await _responseStream.ResponseStream.MoveNext(new CancellationToken());
            }
            catch (RpcException e)
            {
                _rpcErrorAction(e);
                throw new TransportException(e);
            }
        }
    }

    public class InitializationFailureException : Exception
    {
        internal InitializationFailureException(string message) : base(message)
        {
        }

        internal InitializationFailureException(string message, Exception inner) : base(message, inner)
        {
        }
    }

    public class TransportException : Exception
    {
        internal TransportException(RpcException e) : base($"Transport exception: {e.Message}", e)
        {
            Status = ConvertStatus(e.Status);
        }

        public Status Status { get; }
    }
}
