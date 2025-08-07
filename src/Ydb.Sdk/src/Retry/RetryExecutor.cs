using System.Diagnostics;
using Ydb.Sdk.Retry.Classifier;

namespace Ydb.Sdk.Retry;

internal static class RetryExecutor
{
    public static async Task<T> RunAsync<T>(
        Func<CancellationToken, Task<T>> op,
        IRetryPolicy policy,
        bool isIdempotent,
        OperationKind operationKind,
        TimeSpan? overallTimeout = null,
        Func<Task>? recreateSession = null,
        Func<Task>? resetTransport = null,
        IRetryClassifier? classifier = null,
        int? maxAttempts = null,
        CancellationToken ct = default)
    {
        classifier ??= DefaultRetryClassifier.Instance;

        var sw = Stopwatch.StartNew();
        using var tmo = overallTimeout is null ? null : new CancellationTokenSource(overallTimeout.Value);
        using var linked = tmo is null
            ? CancellationTokenSource.CreateLinkedTokenSource(ct)
            : CancellationTokenSource.CreateLinkedTokenSource(ct, tmo.Token);

        var attempt = 0;
        Exception? last = null;

        static Task<T> StartAttempt(Func<CancellationToken, Task<T>> op, CancellationToken token)
        {
            return op(token);
        }

        while (true)
        {
            attempt++;
            if (maxAttempts is { } ma && attempt > ma)
            {
                policy.ReportResult(
                    new RetryContext(
                        attempt,
                        sw.Elapsed,
                        GetTimeLeft(overallTimeout, sw),
                        isIdempotent,
                        operationKind,
                        last is not null ? classifier.Classify(last) : null),
                    success: false);
                throw last ?? new TimeoutException("Retry attempts limit reached.");
            }
            try
            {
                var res = await op(linked.Token).ConfigureAwait(false);

                policy.ReportResult(
                    new RetryContext(
                        attempt,
                        sw.Elapsed,
                        GetTimeLeft(overallTimeout, sw),
                        isIdempotent,
                        operationKind,
                        last is not null ? classifier.Classify(last) : null),
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
                    classifier.Classify(ex)
                );

                var decision = policy.Decide(ctx);

                if (decision.Delay is null)
                {
                    policy.ReportResult(ctx, false);
                    throw;
                }

                if (decision.ResetTransport && resetTransport is not null)
                    await resetTransport().ConfigureAwait(false);

                if (decision.Hedge && ctx.IsIdempotent)
                {
                    var remaining = GetTimeLeft(overallTimeout, sw);
                    if (remaining is { } rem && rem > TimeSpan.Zero)
                    {
                        using var winnerCts = CancellationTokenSource.CreateLinkedTokenSource(linked.Token);

                        using var hedgeCts = CancellationTokenSource.CreateLinkedTokenSource(winnerCts.Token);
                        hedgeCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Max(1, rem.TotalMilliseconds * 0.7)));

                        var hedgedTask = StartAttempt(op, hedgeCts.Token);

                        var primaryTask = Task.Run(async () =>
                        {
                            if (decision.Delay is { } d && d > TimeSpan.Zero)
                            {
                                if (ctx.DeadlineLeft is { } left && d >= left - TimeSpan.FromMilliseconds(50))
                                    throw new TimeoutException("Retry delay exceeds remaining budget.");
                            
                                await Task.Delay(d, linked.Token).ConfigureAwait(false);
                                
                                if (linked.IsCancellationRequested)
                                    throw new TimeoutException("Retry budget exceeded.");
                            }
                            return await StartAttempt(op, winnerCts.Token).ConfigureAwait(false);
                        }, winnerCts.Token);

                        try
                        {
                            var first = await Task.WhenAny(hedgedTask, primaryTask).ConfigureAwait(false);
                            if (first.Status == TaskStatus.RanToCompletion)
                            {
                                var ok = await first.ConfigureAwait(false);
                                try { winnerCts.Cancel(); } catch { /* ignore */ }
                                policy.ReportResult(
                                    new RetryContext(
                                        attempt + 1,
                                        sw.Elapsed,
                                        GetTimeLeft(overallTimeout, sw),
                                        isIdempotent,
                                        operationKind,
                                        classifier.Classify(last)),
                                    true);
                                return ok;
                            }

                            var second = ReferenceEquals(first, hedgedTask) ? primaryTask : hedgedTask;
                            try
                            {
                                var ok2 = await second.ConfigureAwait(false);
                                try { winnerCts.Cancel(); } catch { /* ignore */ }
                                policy.ReportResult(
                                    new RetryContext(
                                        attempt + 1,
                                        sw.Elapsed,
                                        GetTimeLeft(overallTimeout, sw),
                                        isIdempotent,
                                        operationKind,
                                        classifier.Classify(last)),
                                    true);
                                return ok2;
                            }
                            catch
                            {
                                // ignored
                            }
                        }
                        finally
                        {
                            try { winnerCts.Cancel(); }
                            catch
                            {
                                // ignored
                            }

                            _ = Task.WhenAll(
                                hedgedTask.ContinueWith(_ => { }, TaskContinuationOptions.ExecuteSynchronously),
                                primaryTask.ContinueWith(_ => { }, TaskContinuationOptions.ExecuteSynchronously)
                            );
                        }
                        continue;
                    }
                }

                if (decision.RecreateSession && recreateSession is not null)
                    await recreateSession().ConfigureAwait(false);

                if (decision.Delay is { } delay && delay > TimeSpan.Zero)
                    await Task.Delay(delay, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException oce) when (linked.Token.IsCancellationRequested)
            {
                policy.ReportResult(
                    new RetryContext(
                        attempt,
                        sw.Elapsed,
                        GetTimeLeft(overallTimeout, sw),
                        isIdempotent,
                        operationKind,
                        classifier.Classify(oce)
                        ),
                    success: false);
                throw;
            }
        }
    }

    private static TimeSpan? GetTimeLeft(TimeSpan? overall, Stopwatch sw) =>
        overall is null ? null : overall.Value - sw.Elapsed;
}
