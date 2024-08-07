using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Transport;

// TODO Experimental [for Driver with fix call options]
public abstract class GrpcTransport : IDisposable, IAsyncDisposable
{
    protected readonly DriverConfig Config;
    protected readonly ILogger<GrpcTransport> Logger;

    internal GrpcTransport(DriverConfig driverConfig, ILogger<GrpcTransport> logger)
    {
        Logger = logger;
        Config = driverConfig;
    }

    ~GrpcTransport()
    {
        Dispose(false);
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

    internal async Task<TResponse> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        GrpcRequestSettings settings,
        TRequest request
    ) where TRequest : class where TResponse : class
    {
        var (endpoint, channel) = GetChannel(settings.NodeId);
        var callInvoker = channel.CreateCallInvoker();

        Logger.LogTrace("Unary call, method: {MethodName}, endpoint: {Endpoint}", method.Name, endpoint);

        try
        {
            using var call = callInvoker.AsyncUnaryCall(
                method: method,
                host: null,
                options: GetCallOptions(settings, false),
                request: request
            );

            var response = await call.ResponseAsync;
            settings.HeadersHandler(call.GetTrailers());

            return response;
        }
        catch (RpcException e)
        {
            OnRpcError(endpoint, e);
            throw new TransportException(e);
        }
    }

    protected abstract (string, GrpcChannel) GetChannel(long nodeId);

    protected abstract void OnRpcError(string endpoint, RpcException e);

    private CallOptions GetCallOptions(GrpcRequestSettings settings, bool streaming)
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

public class TransportException : Exception
{
    internal TransportException(RpcException e) : base($"Transport exception: {e.Message}", e)
    {
        Status = e.Status.ConvertStatus();
    }

    public Status Status { get; }
}
