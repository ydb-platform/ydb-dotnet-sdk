using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Ado.Benchmarks;

public class MockDriver : IDriver
{
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Dispose()
    {
    }

    public Task<TResponse> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, TRequest request,
        GrpcRequestSettings settings
    ) where TRequest : class where TResponse : class => throw new NotImplementedException();

    public ValueTask<IServerStream<TResponse>> ServerStreamCall<TRequest, TResponse>(Method<TRequest, TResponse> method,
        TRequest request, GrpcRequestSettings settings) where TRequest : class where TResponse : class =>
        throw new NotImplementedException();

    public ValueTask<IBidirectionalStream<TRequest, TResponse>> BidirectionalStreamCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method, GrpcRequestSettings settings)
        where TRequest : class where TResponse : class => throw new NotImplementedException();

    public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
}
