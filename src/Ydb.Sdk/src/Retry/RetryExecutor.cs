using System.Diagnostics;
using Grpc.Core;

namespace Ydb.Sdk.Retry;

public static class RetryExecutor
{
    public static async Task<T> RunAsync<T>(
        Func<CancellationToken, Task<T>> op,
        IRetryPolicy policy,
        bool isIdempotent,
        OperationKind operationKind,
        TimeSpan? overallTimeout = null,
        Func<Task>? recreateSession = null,
        Func<Exception, Failure?>? classify = null,
        CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        using var tmo = overallTimeout is null ? null : new CancellationTokenSource(overallTimeout.Value);
        using var linked = tmo is null
            ? CancellationTokenSource.CreateLinkedTokenSource(ct)
            : CancellationTokenSource.CreateLinkedTokenSource(ct, tmo.Token);

        int attempt = 0;
        Exception? last = null;

        while (true)
        {
            attempt++;
            try
            {
                var res = await op(linked.Token).ConfigureAwait(false);
                policy.ReportResult(new RetryContext(
                        attempt,
                        sw.Elapsed,
                        GetTimeLeft(overallTimeout, sw),
                        isIdempotent,
                        operationKind,
                        last is not null ? ToFailure(last, classify) : null), 
                    true);
                return res;
            }
            catch (Exception ex) when (!linked.Token.IsCancellationRequested)
            {
                last = ex;
                var ctx = new RetryContext(
                    attempt,
                    sw.Elapsed,
                    GetTimeLeft(overallTimeout, sw),
                    isIdempotent,
                    operationKind,
                    ToFailure(ex, classify)
                );
                var decision = policy.Decide(ctx);

                if (decision.Delay is null)
                    throw;

                if (decision.RecreateSession && recreateSession is not null)
                    await recreateSession().ConfigureAwait(false);

                if (decision.Delay.Value > TimeSpan.Zero)
                    await Task.Delay(decision.Delay.Value, linked.Token).ConfigureAwait(false);
            }
        }
    }

    private static TimeSpan? GetTimeLeft(TimeSpan? overall, Stopwatch sw) =>
        overall is null ? null : overall.Value - sw.Elapsed;

    private static Failure? ToFailure(Exception ex, Func<Exception, Failure?>? custom)
    {
        if (custom is not null)
            return custom(ex);

        if (ex is RpcException rx)
            return new Failure(rx, null, (int)rx.StatusCode);

        if (TryGetYdbStatusCode(ex, out var code))
            return new Failure(ex, code);

        return new Failure(ex);
    }

    private static bool TryGetYdbStatusCode(Exception ex, out int ydbStatusCode)
    {
        if (ex.GetType().Name.Contains("Ydb") && ex.Data.Contains("StatusCode") && ex.Data["StatusCode"] is int code)
        {
            ydbStatusCode = code;
            return true;
        }

        ydbStatusCode = default;
        return false;
    }
}