﻿using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;

namespace Ydb.Sdk
{
    public class Driver : IDisposable, IAsyncDisposable
    {
        private readonly DriverConfig _config;
        private readonly ILoggerFactory _loggerFactory;

        private readonly object _lock = new object();
        private readonly ILogger _logger;

        private readonly string _sdkInfo;
        private ChannelsCache _channels;
        private bool _disposed = false;

        public Driver(DriverConfig config, ILoggerFactory? loggerFactory = null)
        {
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _logger = _loggerFactory.CreateLogger<Driver>();
            _config = config;
            _channels = new ChannelsCache(_config, _loggerFactory);

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

        public ILoggerFactory LoggerFactory
        {
            get { return _loggerFactory; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        void Dispose(bool disposing)
        {
            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
            }

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
            _logger.LogInformation("Started initial endpoint discovery");

            try
            {
                var status = await DiscoverEndpoints();
                if (!status.IsSuccess)
                {
                    var error = $"Error during initial endpoint discovery: {status}";
                    _logger.LogCritical(error);
                    throw new InitializationFailureException(error);
                }
            }
            catch (RpcException e)
            {
                _logger.LogCritical($"RPC error during initial endpoint discovery: {e.Status}");
                throw new InitializationFailureException("Failed to discover initial endpoints", e);
            }

            _ = Task.Run(PeriodicDiscovery);
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
                var data = await callInvoker.AsyncUnaryCall(
                    method: method,
                    host: null,
                    options: GetCallOptions(settings, false),
                    request: request);

                return new UnaryResponse<TResponse>(
                    data: data,
                    usedEndpoint: endpoint);
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
                (RpcException e) => { PessimizeEndpoint(endpoint); });
        }

        private async Task<Status> DiscoverEndpoints()
        {
            using var channel = ChannelsCache.CreateChannel(_config.Endpoint, _config, _loggerFactory);

            var client = new Discovery.V1.DiscoveryService.DiscoveryServiceClient(channel);

            var request = new Discovery.ListEndpointsRequest
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
            if (!status.IsSuccess)
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

            var resultProto = response.Operation.Result.Unpack<Ydb.Discovery.ListEndpointsResult>();

            _logger.LogInformation($"Successfully discovered endpoints: {resultProto.Endpoints.Count}" +
                $", self location: {resultProto.SelfLocation}" +
                $", sdk info: {_sdkInfo}");

            _channels.UpdateEndpoints(resultProto);

            return new Status(StatusCode.Success);
        }

        private async Task PeriodicDiscovery()
        {
            bool stop = false;
            while (!stop)
            {
                try
                {
                    await Task.Delay(_config.EndpointDiscoveryInterval);
                    var status = await DiscoverEndpoints();
                }
                catch (RpcException e)
                {
                    _logger.LogWarning($"RPC error during endpoint discovery: {e.Status}");
                }
                catch (Exception e)
                {
                    _logger.LogError($"Unexpected exception during session pool periodic check: {e}");
                }

                lock (_lock)
                {
                    stop = _disposed;
                }
            }
        }

        private void PessimizeEndpoint(string endpoint)
        {
            double? ratio = _channels.PessimizeEndpoint(endpoint);
            if (ratio != null && ratio > _config.PessimizedEndpointRatioTreshold)
            {
                _logger.LogInformation($"Too many pessimized endpoints, initiated endpoint rediscovery.");
                _ = Task.Run(DiscoverEndpoints);
            }
        }

        private Grpc.Core.CallOptions GetCallOptions(RequestSettings settings, bool streaming)
        {
            var meta = new Grpc.Core.Metadata();
            meta.Add(Metadata.RpcDatabaseHeader, _config.Database);

            var authInfo = _config.Credentials.GetAuthInfo();
            if (authInfo != null)
            {
                meta.Add(Metadata.RpcAuthHeader, authInfo);
            }
            if (settings.TraceId.Length > 0)
            {
                meta.Add(Metadata.RpcTraceIdHeader, settings.TraceId);
            }

            TimeSpan transportTimeout = streaming
                ? _config.DefaultStreamingTransportTimeout
                : _config.DefaultTransportTimeout;

            if (settings.TransportTimeout != null)
            {
                transportTimeout = settings.TransportTimeout.Value;
            }

            CallOptions options = new CallOptions(
                headers: meta
            );

            if (transportTimeout != TimeSpan.Zero)
            {
                options = options.WithDeadline(DateTime.UtcNow + transportTimeout);
            }

            return options;
        }

        private static StatusCode ConvertStatusCode(Grpc.Core.StatusCode rpcStatusCode)
        {
            switch (rpcStatusCode)
            {
                case Grpc.Core.StatusCode.Unavailable:
                    return StatusCode.ClientTransportUnavailable;
                case Grpc.Core.StatusCode.DeadlineExceeded:
                    return StatusCode.ClientTransportTimeout;
                case Grpc.Core.StatusCode.ResourceExhausted:
                    return StatusCode.ClientTransportResourceExhausted;
                case Grpc.Core.StatusCode.Unimplemented:
                    return StatusCode.ClientTransportUnimplemented;
                default:
                    return StatusCode.ClientTransportUnknown;
            }
        }

        private static Status ConvertStatus(Grpc.Core.Status rpcStatus)
        {
            return new Status(
                ConvertStatusCode(rpcStatus.StatusCode),
                new List<Issue> { new Issue(rpcStatus.Detail) });
        }

        internal sealed class UnaryResponse<TResponse>
        {
            internal UnaryResponse(
                TResponse data,
                string usedEndpoint)
            {
                Data = data;
                UsedEndpoint = usedEndpoint;
            }

            public TResponse Data { get; }

            public string UsedEndpoint { get; }
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

            public TResponse Response
            {
                get
                {
                    return _responseStream.ResponseStream.Current;
                }
            }

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
            internal InitializationFailureException(string message)
                : base(message)
            {
            }

            internal InitializationFailureException(string message, Exception inner)
                : base(message, inner)
            {
            }
        }

        public class TransportException : Exception
        {
            internal TransportException(RpcException e)
                : base($"Transport exception: {e.Message}", e)
            {
                Status = ConvertStatus(e.Status);
            }

            public Status Status { get; }
        }
    }
}
