using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk;

// TODO Experimental [for Driver with fix call options]
public abstract class GrpcTransport : IDisposable, IAsyncDisposable
{
    public ILoggerFactory LoggerFactory { get; }

    protected readonly ILogger Logger;
    protected readonly DriverConfig Config;

    protected volatile bool Disposed;

    internal GrpcTransport(DriverConfig driverConfig, ILoggerFactory? loggerFactory)
    {
        LoggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        Logger = LoggerFactory.CreateLogger<GrpcTransport>();
        Config = driverConfig;
    }

    ~GrpcTransport()
    {
        Dispose(Disposed);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public ValueTask DisposeAsync()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
        return default;
    }

    protected abstract void Dispose(bool disposing);

    internal async Task<Driver.UnaryResponse<TResponse>> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        RequestSettings settings,
        TRequest request,
        string? preferredEndpoint = null
    ) where TRequest : class where TResponse : class
    {
        var (endpoint, channel) = GetChannel(preferredEndpoint);
        var callInvoker = channel.CreateCallInvoker();

        Logger.LogTrace($"Unary call" +
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

            return new Driver.UnaryResponse<TResponse>(
                data: data,
                usedEndpoint: endpoint,
                trailers: trailers
            );
        }
        catch (RpcException e)
        {
            OnRpcError(endpoint);
            throw new Driver.TransportException(e); // TODO fix 
        }
    }

    protected abstract (string, GrpcChannel) GetChannel(string? preferredEndpoint);

    protected abstract void OnRpcError(string endpoint);

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
}
