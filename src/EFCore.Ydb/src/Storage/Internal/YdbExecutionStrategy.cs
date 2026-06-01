using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Tracing;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

/// <summary>
/// Retry strategy for YDB.<br/>
///
/// <br/>IMPORTANT:
/// <br/>- Whether/how long to retry is fully delegated to the supplied <see cref="IRetryPolicy"/>.
/// The base ExecutionStrategy parameters (maxRetryCount, maxRetryDelay) are not used; we pass
/// <see cref="int.MaxValue"/> / <see cref="TimeSpan.Zero"/> as placeholders.
/// <br/>- This strategy must be invoked in the correct EF Core context/connection (YDB),
/// so that exception types and ShouldRetryOn semantics match the provider.
/// <br/>- Each call is wrapped in a <c>ydb.RunWithRetry</c> activity and reports the SDK retry
/// histograms (<c>ydb.client.retry.duration</c>, <c>ydb.client.retry.attempts</c>); each attempt
/// is wrapped in its own <c>ydb.Try</c> child span, with <c>ydb.retry.backoff_ms</c> set on retry
/// attempts.
/// </summary>
public class YdbExecutionStrategy(ExecutionStrategyDependencies dependencies, IRetryPolicy retryPolicy)
    : ExecutionStrategy(dependencies, int.MaxValue /* unused */, TimeSpan.Zero /* unused */)
{
    private const string ActivityName = "ydb.RunWithRetry";

    private Activity? _currentTryActivity;
    private int _retryCount;

    public override bool RetriesOnFailure => true;

    protected override bool ShouldRetryOn(Exception exception) =>
        exception is YdbException ydbException && retryPolicy.GetNextDelay(ydbException, attempt: 0) is not null;

    protected override TimeSpan? GetNextDelay(Exception lastException)
    {
        var delay = retryPolicy.GetNextDelay((YdbException)lastException, ExceptionsEncountered.Count - 1);
        if (delay is null)
        {
            return null;
        }

        // Close the previous attempt's span and open a new one so the next operation
        // (and the Task.Delay before it) is parented under the fresh `ydb.Try`.
        _currentTryActivity?.SetException(lastException);
        _currentTryActivity?.Dispose();
        _currentTryActivity = YdbActivitySource.StartActivity("ydb.Try", ActivityKind.Internal);
        _currentTryActivity?.SetRetryAttributes(delay.Value);

        return delay;
    }

    protected override void OnRetry()
    {
        base.OnRetry();
        _retryCount++;
    }

    public override TResult Execute<TState, TResult>(
        TState state,
        Func<DbContext, TState, TResult> operation,
        Func<DbContext, TState, ExecutionResult<TResult>>? verifySucceeded
    )
    {
        using var dbActivity = YdbActivitySource.StartActivity(ActivityName, ActivityKind.Internal);
        var startTimestamp = YdbMetricsReporter.ReportRetryStart();
        BeginRetryLoop();
        try
        {
            return base.Execute(state, operation, verifySucceeded);
        }
        catch (Exception e)
        {
            _currentTryActivity?.SetException(e);
            dbActivity?.SetException(e);
            throw;
        }
        finally
        {
            EndRetryLoop(startTimestamp);
        }
    }

    public override async Task<TResult> ExecuteAsync<TState, TResult>(
        TState state,
        Func<DbContext, TState, CancellationToken, Task<TResult>> operation,
        Func<DbContext, TState, CancellationToken, Task<ExecutionResult<TResult>>>? verifySucceeded,
        CancellationToken cancellationToken = default
    )
    {
        using var dbActivity = YdbActivitySource.StartActivity(ActivityName, ActivityKind.Internal);
        var startTimestamp = YdbMetricsReporter.ReportRetryStart();
        BeginRetryLoop();
        try
        {
            return await base.ExecuteAsync(state, operation, verifySucceeded, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _currentTryActivity?.SetException(e);
            dbActivity?.SetException(e);
            throw;
        }
        finally
        {
            EndRetryLoop(startTimestamp);
        }
    }

    private void BeginRetryLoop()
    {
        _retryCount = 0;
        _currentTryActivity = YdbActivitySource.StartActivity("ydb.Try", ActivityKind.Internal);
    }

    private void EndRetryLoop(long startTimestamp)
    {
        _currentTryActivity?.Dispose();
        _currentTryActivity = null;
        YdbMetricsReporter.ReportRetryStop(startTimestamp, _retryCount + 1, operationName: null);
    }
}
