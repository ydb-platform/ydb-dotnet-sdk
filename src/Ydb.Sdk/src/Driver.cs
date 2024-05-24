using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Discovery;
using Ydb.Discovery.V1;

namespace Ydb.Sdk;

public class Driver : IDisposable, IAsyncDisposable
{
    private readonly DriverConfig _config;
    private readonly ILogger _logger;
    private readonly string _sdkInfo;
    private readonly ChannelsCache _channels;

    private volatile bool _disposed;

    internal ILoggerFactory LoggerFactory { get; }
    internal string Database => _config.Database;

    public Driver(DriverConfig config, ILoggerFactory? loggerFactory = null)
    {
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = LoggerFactory.CreateLogger<Driver>();
        _config = config;
        _channels = new ChannelsCache(_config, LoggerFactory);

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

    ~Driver()
    {
        Dispose(_disposed);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        _disposed = true;

        if (disposing)
        {
            _channels.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
        return default;
    }

    public async Task Initialize()
    {
        await _config.Credentials.ProvideConfig(_config);

        _logger.LogInformation("Started initial endpoint discovery");

        for (var i = 0; i < _config.AttemptDiscovery; i++)
        {
            try
            {
                var status = await DiscoverEndpoints();
                if (status.IsSuccess)
                {
                    _ = Task.Run(PeriodicDiscovery);

                    return;
                }

                _logger.LogCritical($"Error during initial endpoint discovery: {status}");
            }
            catch (RpcException e)
            {
                _logger.LogCritical($"RPC error during initial endpoint discovery: {e.Status}");

                if (i == _config.AttemptDiscovery - 1)
                {
                    throw;
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(i * 200)); // await 0 ms, 200 ms, 400ms, .. 1.8 sec
        }

        throw new InitializationFailureException("Error during initial endpoint discovery");
    }

    internal async Task<UnaryResponse<TResponse>> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        RequestSettings settings,
        string? preferredEndpoint = null)
        where TRequest : class
        where TResponse : class
    {
        var (endpoint, channel) = _channels.GetChannel(preferredEndpoint);
        var callInvoker = channel.CreateCallInvoker();

        _logger.LogTrace($"Unary call" +
                         $", method: {method.Name}" +
                         $", endpoint: {endpoint}");

        try
        {
            using var call = callInvoker.AsyncUnaryCall(
                method: method,
                host: null,
                options: GetCallOptions(settings, false),
                request: request);

            var data = await call.ResponseAsync;
            var trailers = call.GetTrailers();

            return new UnaryResponse<TResponse>(
                data: data,
                usedEndpoint: endpoint,
                trailers: trailers);
        }
        catch (RpcException e)
        {
            PessimizeEndpoint(endpoint);
            throw new TransportException(e);
        }
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
            _ => { PessimizeEndpoint(endpoint); });
    }

    private async Task<Status> DiscoverEndpoints()
    {
        using var channel = ChannelsCache.CreateChannel(_config.Endpoint, _config, LoggerFactory);

        var client = new DiscoveryService.DiscoveryServiceClient(channel);

        var request = new ListEndpointsRequest
        {
            Database = _config.Database
        };

        var requestSettings = new RequestSettings
        {
            TransportTimeout = _config.EndpointDiscoveryTimeout
        };

        var options = GetCallOptions(requestSettings, false);
        options.Headers.Add(Metadata.RpcSdkInfoHeader, _sdkInfo);

        var response = await client.ListEndpointsAsync(
            request: request,
            options: options);

        if (!response.Operation.Ready)
        {
            var error = "Unexpected non-ready endpoint discovery operation.";
            _logger.LogError($"Endpoint discovery internal error: {error}");

            return new Status(StatusCode.ClientInternalError, error);
        }

        var status = Status.FromProto(response.Operation.Status, response.Operation.Issues);
        if (status.IsNotSuccess)
        {
            _logger.LogWarning($"Unsuccessful endpoint discovery: {status}");
            return status;
        }

        if (response.Operation.Result is null)
        {
            var error = "Unexpected empty endpoint discovery result.";
            _logger.LogError($"Endpoint discovery internal error: {error}");

            return new Status(StatusCode.ClientInternalError, error);
        }

        var resultProto = response.Operation.Result.Unpack<ListEndpointsResult>();

        _logger.LogInformation($"Successfully discovered endpoints: {resultProto.Endpoints.Count}" +
                               $", self location: {resultProto.SelfLocation}" +
                               $", sdk info: {_sdkInfo}");

        _channels.UpdateEndpoints(resultProto);

        return new Status(StatusCode.Success);
    }

    private async Task PeriodicDiscovery()
    {
        while (!_disposed)
        {
            try
            {
                await Task.Delay(_config.EndpointDiscoveryInterval);
                _ = await DiscoverEndpoints();
            }
            catch (RpcException e)
            {
                _logger.LogWarning($"RPC error during endpoint discovery: {e.Status}");
            }
            catch (Exception e)
            {
                _logger.LogError($"Unexpected exception during session pool periodic check: {e}");
            }
        }
    }

    private void PessimizeEndpoint(string endpoint)
    {
        var ratio = _channels.PessimizeEndpoint(endpoint);
        if (ratio != null && ratio > _config.PessimizedEndpointRatioTreshold)
        {
            _logger.LogInformation("Too many pessimized endpoints, initiated endpoint rediscovery.");
            _ = Task.Run(DiscoverEndpoints);
        }
    }

    private CallOptions GetCallOptions(RequestSettings settings, bool streaming)
    {
        var meta = new Grpc.Core.Metadata
        {
            { Metadata.RpcDatabaseHeader, _config.Database }
        };

        var authInfo = _config.Credentials.GetAuthInfo();
        if (authInfo != null)
        {
            meta.Add(Metadata.RpcAuthHeader, authInfo);
        }

        if (settings.TraceId.Length > 0)
        {
            meta.Add(Metadata.RpcTraceIdHeader, settings.TraceId);
        }

        var transportTimeout = streaming
            ? _config.DefaultStreamingTransportTimeout
            : _config.DefaultTransportTimeout;

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
            Status = e.Status.ConvertStatus();
        }

        public Status Status { get; }
    }
}
