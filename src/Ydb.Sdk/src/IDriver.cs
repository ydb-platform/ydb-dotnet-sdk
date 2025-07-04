using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Services.Auth;

namespace Ydb.Sdk;

public interface IDriver : IAsyncDisposable, IDisposable
{
    public Task<TResponse> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class;

    public ValueTask<ServerStream<TResponse>> ServerStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class;

    public ValueTask<IBidirectionalStream<TRequest, TResponse>> BidirectionalStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class;

    ILoggerFactory LoggerFactory { get; }
}

public interface IBidirectionalStream<in TRequest, out TResponse> : IDisposable
{
    public Task Write(TRequest request);

    public ValueTask<bool> MoveNextAsync();

    public TResponse Current { get; }

    public ValueTask<string?> AuthToken();

    public Task RequestStreamComplete();
}

public interface IServerStream<out TResponse> : IDisposable
{
    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default);

    public TResponse Current { get; }
}

public abstract class BaseDriver : IDriver
{
    private readonly ICredentialsProvider? _credentialsProvider;

    protected readonly DriverConfig Config;
    protected readonly ILogger Logger;

    internal readonly GrpcChannelFactory GrpcChannelFactory;
    internal readonly ChannelPool<GrpcChannel> ChannelPool;

    protected int Disposed;

    internal BaseDriver(
        DriverConfig config,
        ILoggerFactory loggerFactory,
        ILogger logger
    )
    {
        Config = config;
        Logger = logger;
        LoggerFactory = loggerFactory;

        GrpcChannelFactory = new GrpcChannelFactory(LoggerFactory, Config);
        ChannelPool = new ChannelPool<GrpcChannel>(LoggerFactory, GrpcChannelFactory);

        _credentialsProvider = Config.User != null
            ? new CachedCredentialsProvider(
                new StaticCredentialsAuthClient(Config, GrpcChannelFactory, LoggerFactory),
                LoggerFactory
            )
            : Config.Credentials;
    }

    public async Task<TResponse> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class
    {
        var endpoint = GetEndpoint(settings.NodeId);
        var channel = ChannelPool.GetChannel(endpoint);

        var callInvoker = channel.CreateCallInvoker();

        Logger.LogTrace("Unary call, method: {MethodName}, endpoint: {Endpoint}", method.Name, endpoint);

        try
        {
            using var call = callInvoker.AsyncUnaryCall(
                method: method,
                host: null,
                options: await GetCallOptions(settings),
                request: request
            );

            var response = await call.ResponseAsync;
            settings.TrailersHandler(call.GetTrailers());

            return response;
        }
        catch (RpcException e)
        {
            OnRpcError(endpoint, e);
            throw new Driver.TransportException(e);
        }
    }

    public async ValueTask<ServerStream<TResponse>> ServerStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class
    {
        var endpoint = GetEndpoint(settings.NodeId);
        var channel = ChannelPool.GetChannel(endpoint);

        var callInvoker = channel.CreateCallInvoker();

        var call = callInvoker.AsyncServerStreamingCall(
            method: method,
            host: null,
            options: await GetCallOptions(settings),
            request: request);

        return new ServerStream<TResponse>(call, e => { OnRpcError(endpoint, e); });
    }

    public async ValueTask<IBidirectionalStream<TRequest, TResponse>> BidirectionalStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class
    {
        var endpoint = GetEndpoint(settings.NodeId);
        var channel = ChannelPool.GetChannel(endpoint);

        var callInvoker = channel.CreateCallInvoker();

        var call = callInvoker.AsyncDuplexStreamingCall(
            method: method,
            host: null,
            options: await GetCallOptions(settings));

        return new BidirectionalStream<TRequest, TResponse>(
            call,
            e => { OnRpcError(endpoint, e); },
            _credentialsProvider
        );
    }

    protected abstract string GetEndpoint(long nodeId);

    protected abstract void OnRpcError(string endpoint, RpcException e);

    protected async ValueTask<CallOptions> GetCallOptions(GrpcRequestSettings settings)
    {
        var meta = Config.GetCallMetadata;

        if (_credentialsProvider != null)
        {
            meta.Add(Metadata.RpcAuthHeader, await _credentialsProvider.GetAuthInfoAsync());
        }

        if (settings.TraceId.Length > 0)
        {
            meta.Add(Metadata.RpcTraceIdHeader, settings.TraceId);
        }
        
        foreach (var setting in settings.List) {
            meta.Add(Metadata.RpcClientCapabilitiesHeader, "session-balancer");
        }
        
        var options = new CallOptions(headers: meta, cancellationToken: settings.CancellationToken);

        if (settings.TransportTimeout != TimeSpan.Zero)
        {
            options = options.WithDeadline(DateTime.UtcNow + settings.TransportTimeout);
        }

        return options;
    }

    public ILoggerFactory LoggerFactory { get; }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref Disposed, 1, 0) == 0)
        {
            await ChannelPool.DisposeAsync();

            GC.SuppressFinalize(this);
        }
    }
}

public sealed class ServerStream<TResponse> : IServerStream<TResponse>
{
    private readonly AsyncServerStreamingCall<TResponse> _stream;
    private readonly Action<RpcException> _rpcErrorAction;

    internal ServerStream(AsyncServerStreamingCall<TResponse> stream, Action<RpcException> rpcErrorAction)
    {
        _stream = stream;
        _rpcErrorAction = rpcErrorAction;
    }

    public async ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _stream.ResponseStream.MoveNext(cancellationToken);
        }
        catch (RpcException e)
        {
            _rpcErrorAction(e);

            throw new Driver.TransportException(e);
        }
    }

    public TResponse Current => _stream.ResponseStream.Current;

    public void Dispose() => _stream.Dispose();
}

internal class BidirectionalStream<TRequest, TResponse> : IBidirectionalStream<TRequest, TResponse>
{
    private readonly AsyncDuplexStreamingCall<TRequest, TResponse> _stream;
    private readonly Action<RpcException> _rpcErrorAction;
    private readonly ICredentialsProvider? _credentialsProvider;

    internal BidirectionalStream(
        AsyncDuplexStreamingCall<TRequest, TResponse> stream,
        Action<RpcException> rpcErrorAction,
        ICredentialsProvider? credentialsProvider)
    {
        _stream = stream;
        _rpcErrorAction = rpcErrorAction;
        _credentialsProvider = credentialsProvider;
    }

    public async Task Write(TRequest request)
    {
        try
        {
            await _stream.RequestStream.WriteAsync(request);
        }
        catch (RpcException e)
        {
            _rpcErrorAction(e);

            throw new Driver.TransportException(e);
        }
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        try
        {
            return await _stream.ResponseStream.MoveNext(CancellationToken.None);
        }
        catch (RpcException e)
        {
            _rpcErrorAction(e);

            throw new Driver.TransportException(e);
        }
    }

    public TResponse Current => _stream.ResponseStream.Current;

    public async ValueTask<string?> AuthToken() => _credentialsProvider != null
        ? await _credentialsProvider.GetAuthInfoAsync()
        : null;

    public async Task RequestStreamComplete()
    {
        try
        {
            await _stream.RequestStream.CompleteAsync();
        }
        catch (RpcException e)
        {
            _rpcErrorAction(e);
        }
    }

    public void Dispose() => _stream.Dispose();
}
