using Grpc.Core;
using Grpc.Core.Interceptors;

using GrpcStatus = Grpc.Core.Status;
using GrpcMetadata = Grpc.Core.Metadata;
using GrpcStatusCode = Grpc.Core.StatusCode;
using GrpcIMethod = Grpc.Core.IMethod;

using Ydb.Sdk.Retry.Classifier;

namespace Ydb.Sdk.Retry;

internal sealed class RetryingInterceptor : Interceptor
{
    private readonly IRetryPolicy _policy;
    private readonly TimeSpan _defaultDeadline;
    private readonly IRetryClassifier _classifier;
    private readonly int? _maxAttempts;

    public Func<Task>? ResetTransport { get; init; }

    public RetryingInterceptor(
        IRetryPolicy policy,
        TimeSpan? defaultDeadline = null,
        IRetryClassifier? classifier = null,
        int? maxAttempts = null)
    {
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _defaultDeadline = defaultDeadline ?? TimeSpan.FromSeconds(30);
        _classifier = classifier ?? DefaultRetryClassifier.Instance;
        _maxAttempts = maxAttempts ?? YdbRetryPolicy.DefaultMaxAttempts;
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        bool isRead = GuessIdempotency(context.Method);
        var kind = Classify(context.Method);

        var budget = context.Options.Deadline is { } dl
            ? dl - DateTime.UtcNow
            : _defaultDeadline;

        var startedAt = DateTime.UtcNow;

        Func<GrpcStatus>   statusProvider    = () => new GrpcStatus(GrpcStatusCode.OK, "");
        Func<GrpcMetadata> trailersProvider  = () => new GrpcMetadata();
        Action             disposeProvider   = () => { };

        Task<GrpcMetadata> headersTask = Task.FromResult(new GrpcMetadata());

        async Task<TResponse> Do(CancellationToken ct)
        {
            var elapsed   = DateTime.UtcNow - startedAt;
            var remaining = budget - elapsed;
            if (remaining <= TimeSpan.FromMilliseconds(50))
                throw new RpcException(new GrpcStatus(GrpcStatusCode.DeadlineExceeded, "retry budget exceeded"));

            var opts = context.Options
                .WithCancellationToken(ct)
                .WithDeadline(DateTime.UtcNow + remaining);

            var ctx2 = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, opts);
            var call = continuation(request, ctx2);

            statusProvider   = call.GetStatus;
            trailersProvider = call.GetTrailers;
            disposeProvider  = call.Dispose;
            headersTask = call.ResponseHeadersAsync;

            return await call.ResponseAsync.ConfigureAwait(false);
        }

        var responseTask = RetryExecutor.RunAsync(
            op: Do,
            policy: _policy,
            isIdempotent: isRead,
            operationKind: kind,
            overallTimeout: null,
            recreateSession: null,
            resetTransport: ResetTransport,
            classifier: _classifier,
            maxAttempts: _maxAttempts
        );

        return new AsyncUnaryCall<TResponse>(
            responseTask,
            headersTask,
            () => statusProvider(),
            () => trailersProvider(),
            () => disposeProvider()
        );
    }

    private static bool GuessIdempotency(GrpcIMethod method) =>
        method.Type == MethodType.Unary &&
        (method.FullName.Contains("Read", StringComparison.OrdinalIgnoreCase)
         || method.FullName.Contains("Describe", StringComparison.OrdinalIgnoreCase)
         || method.FullName.Contains("Get", StringComparison.OrdinalIgnoreCase));

    private static OperationKind Classify(GrpcIMethod method)
    {
        var n = method.FullName;
        if (n.Contains("Topic", StringComparison.OrdinalIgnoreCase)) return OperationKind.Stream;
        if (n.Contains("Scheme", StringComparison.OrdinalIgnoreCase)) return OperationKind.Schema;
        if (n.Contains("Discovery", StringComparison.OrdinalIgnoreCase)) return OperationKind.Discovery;
        return OperationKind.Read;
    }
}
