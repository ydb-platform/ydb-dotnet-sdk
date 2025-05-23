using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;

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

public abstract class BaseDriver : IDriver
{
    protected readonly DriverConfig Config;
    protected readonly ILogger Logger;

    protected int Disposed;

    protected BaseDriver(DriverConfig config, ILoggerFactory loggerFactory, ILogger logger)
    {
        Config = config;
        Logger = logger;
        LoggerFactory = loggerFactory;
    }

    public async Task<TResponse> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class
    {
        var (endpoint, channel) = GetChannel(settings.NodeId);
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
        var (endpoint, channel) = GetChannel(settings.NodeId);
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
        var (endpoint, channel) = GetChannel(settings.NodeId);
        var callInvoker = channel.CreateCallInvoker();

        var call = callInvoker.AsyncDuplexStreamingCall(
            method: method,
            host: null,
            options: await GetCallOptions(settings));

        return new BidirectionalStream<TRequest, TResponse>(
            call,
            e => { OnRpcError(endpoint, e); },
            CredentialsProvider
        );
    }

    protected abstract (string, GrpcChannel) GetChannel(long nodeId);

    protected abstract void OnRpcError(string endpoint, RpcException e);

    protected async ValueTask<CallOptions> GetCallOptions(GrpcRequestSettings settings)
    {
        var meta = new Grpc.Core.Metadata
        {
            { Metadata.RpcDatabaseHeader, Config.Database },
            { Metadata.RpcSdkInfoHeader, Config.SdkVersion }
        };

        if (CredentialsProvider != null)
        {
            meta.Add(Metadata.RpcAuthHeader, await CredentialsProvider.GetAuthInfoAsync());
        }

        if (settings.TraceId.Length > 0)
        {
            meta.Add(Metadata.RpcTraceIdHeader, settings.TraceId);
        }

        var options = new CallOptions(
            headers: meta,
            cancellationToken: settings.CancellationToken
        );

        if (settings.TransportTimeout != TimeSpan.Zero)
        {
            options = options.WithDeadline(DateTime.UtcNow + settings.TransportTimeout);
        }

        return options;
    }

    protected abstract ICredentialsProvider? CredentialsProvider { get; }

    public ILoggerFactory LoggerFactory { get; }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref Disposed, 1, 0) == 0)
        {
            await InternalDispose();
        }
    }

    protected abstract ValueTask InternalDispose();
}

public sealed class ServerStream<TResponse> : IAsyncEnumerator<TResponse>, IAsyncEnumerable<TResponse>
{
    private readonly AsyncServerStreamingCall<TResponse> _stream;
    private readonly Action<RpcException> _rpcErrorAction;

    internal ServerStream(AsyncServerStreamingCall<TResponse> stream, Action<RpcException> rpcErrorAction)
    {
        _stream = stream;
        _rpcErrorAction = rpcErrorAction;
    }

    public ValueTask DisposeAsync()
    {
        _stream.Dispose();

        return default;
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

    public IAsyncEnumerator<TResponse> GetAsyncEnumerator(CancellationToken cancellationToken = new()) => this;
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
