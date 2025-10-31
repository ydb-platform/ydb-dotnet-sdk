using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Services.Auth;

namespace Ydb.Sdk;

/// <summary>
/// Core interface for YDB driver operations.
/// </summary>
/// <remarks>
/// The IDriver interface defines the contract for YDB client drivers.
/// It provides methods for executing gRPC calls and managing driver lifecycle.
/// </remarks>
public interface IDriver : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Executes a unary gRPC call.
    /// </summary>
    /// <typeparam name="TRequest">The request message type.</typeparam>
    /// <typeparam name="TResponse">The response message type.</typeparam>
    /// <param name="method">The gRPC method to call.</param>
    /// <param name="request">The request message.</param>
    /// <param name="settings">gRPC request settings.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response message.</returns>
    public Task<TResponse> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class;

    /// <summary>
    /// Executes a server streaming gRPC call.
    /// </summary>
    /// <typeparam name="TRequest">The request message type.</typeparam>
    /// <typeparam name="TResponse">The response message type.</typeparam>
    /// <param name="method">The gRPC method to call.</param>
    /// <param name="request">The request message.</param>
    /// <param name="settings">gRPC request settings.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the server stream.</returns>
    public ValueTask<IServerStream<TResponse>> ServerStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class;

    /// <summary>
    /// Executes a bidirectional streaming gRPC call.
    /// </summary>
    /// <typeparam name="TRequest">The request message type.</typeparam>
    /// <typeparam name="TResponse">The response message type.</typeparam>
    /// <param name="method">The gRPC method to call.</param>
    /// <param name="settings">gRPC request settings.</param>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the bidirectional stream.</returns>
    public ValueTask<IBidirectionalStream<TRequest, TResponse>> BidirectionalStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        GrpcRequestSettings settings)
        where TRequest : class
        where TResponse : class;

    /// <summary>
    /// Gets the logger factory used by this driver.
    /// </summary>
    ILoggerFactory LoggerFactory { get; }

    /// <summary>
    /// Registers a new owner of this driver instance.
    /// </summary>
    /// <remarks>
    /// This method is used to track how many components are using this driver.
    /// The driver will not be disposed until all owners are released.
    /// </remarks>
    void RegisterOwner();

    /// <summary>
    /// Gets a value indicating whether this driver has been disposed.
    /// </summary>
    bool IsDisposed { get; }
}

/// <summary>
/// Represents a bidirectional gRPC stream for sending requests and receiving responses.
/// </summary>
/// <typeparam name="TRequest">The type of request messages.</typeparam>
/// <typeparam name="TResponse">The type of response messages.</typeparam>
public interface IBidirectionalStream<in TRequest, out TResponse> : IDisposable
{
    /// <summary>
    /// Writes a request message to the stream.
    /// </summary>
    /// <param name="request">The request message to write.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    public Task Write(TRequest request);

    /// <summary>
    /// Advances the stream to the next response message.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation. The task result indicates whether a response was available.</returns>
    public Task<bool> MoveNextAsync();

    /// <summary>
    /// Gets the current response message.
    /// </summary>
    public TResponse Current { get; }

    /// <summary>
    /// Gets the current authentication token.
    /// </summary>
    /// <returns>A value task that represents the asynchronous operation. The task result contains the authentication token.</returns>
    public ValueTask<string?> AuthToken();

    /// <summary>
    /// Completes the request stream.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public Task RequestStreamComplete();
}

/// <summary>
/// Represents a server streaming gRPC stream for receiving response messages.
/// </summary>
/// <typeparam name="TResponse">The type of response messages.</typeparam>
public interface IServerStream<out TResponse> : IDisposable
{
    /// <summary>
    /// Advances the stream to the next response message.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result indicates whether a response was available.</returns>
    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the current response message.
    /// </summary>
    public TResponse Current { get; }
}

/// <summary>
/// Base implementation of the IDriver interface providing common functionality.
/// </summary>
/// <remarks>
/// BaseDriver provides the core implementation for YDB drivers including gRPC channel management,
/// authentication handling, request metadata management, and error handling.
/// </remarks>
public abstract class BaseDriver : IDriver
{
    private readonly ICredentialsProvider? _credentialsProvider;

    protected readonly DriverConfig Config;
    protected readonly ILogger Logger;

    internal readonly GrpcChannelFactory GrpcChannelFactory;
    internal readonly ChannelPool<GrpcChannel> ChannelPool;

    private int _ownerCount;

    protected volatile int Disposed;

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

            throw new YdbException(e);
        }
        catch (Exception e)
        {
            throw new YdbException(StatusCode.ClientTransportUnknown, "Unexpected transport exception", e);
        }
    }

    public async ValueTask<IServerStream<TResponse>> ServerStreamCall<TRequest, TResponse>(
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

        foreach (var clientCapabilitiesHeader in settings.ClientCapabilities)
        {
            meta.Add(Metadata.RpcClientCapabilitiesHeader, clientCapabilitiesHeader);
        }

        var options = new CallOptions(headers: meta, cancellationToken: settings.CancellationToken);

        if (settings.TransportTimeout != TimeSpan.Zero)
        {
            options = options.WithDeadline(DateTime.UtcNow + settings.TransportTimeout);
        }

        return options;
    }

    public ILoggerFactory LoggerFactory { get; }
    public void RegisterOwner() => Interlocked.Increment(ref _ownerCount);
    public bool IsDisposed => Disposed == 1;

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Decrement(ref _ownerCount) <= 0 && Interlocked.CompareExchange(ref Disposed, 1, 0) == 0)
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

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _stream.ResponseStream.MoveNext(cancellationToken);
        }
        catch (RpcException e)
        {
            _rpcErrorAction(e);

            throw new YdbException(e);
        }
        catch (Exception e)
        {
            throw new YdbException(StatusCode.ClientTransportUnknown, "Unexpected transport exception", e);
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

            throw new YdbException(e);
        }
        catch (Exception e)
        {
            throw new YdbException(StatusCode.ClientTransportUnknown, "Unexpected transport exception", e);
        }
    }

    public async Task<bool> MoveNextAsync()
    {
        try
        {
            return await _stream.ResponseStream.MoveNext(CancellationToken.None);
        }
        catch (RpcException e)
        {
            _rpcErrorAction(e);

            throw new YdbException(e);
        }
        catch (Exception e)
        {
            throw new YdbException(StatusCode.ClientTransportUnknown, "Unexpected transport exception", e);
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
