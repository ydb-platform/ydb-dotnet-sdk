using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Ydb.Sdk.Retry;

public sealed class RetryingInterceptor : Interceptor
{
    private readonly IRetryPolicy _policy;
    private readonly TimeSpan _defaultDeadline;

    public RetryingInterceptor(IRetryPolicy policy, TimeSpan? defaultDeadline = null)
    {
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _defaultDeadline = defaultDeadline ?? TimeSpan.FromSeconds(30);
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        bool isRead = GuessIdempotency(context.Method);
        var kind = Classify(context.Method);

        var deadlineLeft = context.Options.Deadline is { } dl
            ? dl - DateTime.UtcNow
            : _defaultDeadline;

        async Task<TResponse> Do(CancellationToken ct)
        {
            var call = continuation(request, context);
            await using var registration = ct.Register(() =>
            {
                try { call.Dispose(); }
                catch
                {}
            });
            return await call.ResponseAsync.ConfigureAwait(false);
        }

        var task = RetryExecutor.RunAsync(
            op: Do,
            policy: _policy,
            isIdempotent: isRead,
            operationKind: kind,
            overallTimeout: deadlineLeft
        );

        return new AsyncUnaryCall<TResponse>(task, null, null, null, null);
    }

    private static bool GuessIdempotency(IMethod method) =>
        method.Type == MethodType.Unary &&
        (method.FullName.Contains("Read", StringComparison.OrdinalIgnoreCase)
         || method.FullName.Contains("Describe", StringComparison.OrdinalIgnoreCase)
         || method.FullName.Contains("Get", StringComparison.OrdinalIgnoreCase));

    private static OperationKind Classify(IMethod method)
    {
        var n = method.FullName;
        if (n.Contains("Topic", StringComparison.OrdinalIgnoreCase)) return OperationKind.Stream;
        if (n.Contains("Scheme", StringComparison.OrdinalIgnoreCase)) return OperationKind.Schema;
        if (n.Contains("Discovery", StringComparison.OrdinalIgnoreCase)) return OperationKind.Discovery;
        return OperationKind.Read;
    }
}