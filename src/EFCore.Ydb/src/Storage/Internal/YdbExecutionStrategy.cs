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
/// <br/>- The maximum number of attempts and backoff logic are encapsulated in <see cref="YdbRetryPolicy"/>.
/// The base ExecutionStrategy parameters (maxRetryCount, maxRetryDelay) are not used.
/// <br/>- This strategy must be invoked in the correct EF Core context/connection (YDB),
/// so that exception types and ShouldRetryOn semantics match the provider.
/// <br/>- Each call to <see cref="Execute{TState, TResult}"/>/<see cref="ExecuteAsync{TState, TResult}"/>
/// is wrapped in a <c>ydb.RunWithRetry</c> activity (or <see cref="YdbRetryPolicyConfig.OperationName"/>
/// when set) and reports the SDK retry histograms (<c>ydb.client.retry.duration</c>,
/// <c>ydb.client.retry.attempts</c>); each attempt is wrapped in its own <c>ydb.Try</c> child span,
/// with <c>ydb.retry.backoff_ms</c> set on retry attempts.
/// </summary>
public class YdbExecutionStrategy(ExecutionStrategyDependencies dependencies, YdbRetryPolicyConfig retryPolicyConfig)
// Placeholders forwarded to the base class:
// - MaxAttempts and TimeSpan.Zero are not used; YdbRetryPolicy drives the real limits/delays.
    : ExecutionStrategy(dependencies, retryPolicyConfig.MaxAttempts, TimeSpan.Zero /* unused */)
{
    private const string DefaultActivityName = "ydb.RunWithRetry";
    private const string TryActivityName = "ydb.Try";

    private readonly YdbRetryPolicy _retryPolicy = new(retryPolicyConfig);
    private readonly string _activityName = retryPolicyConfig.OperationName ?? DefaultActivityName;

    private Activity? _currentTryActivity;
    private int _retryCount;

    public override bool RetriesOnFailure => true;

    protected override bool ShouldRetryOn(Exception exception) =>
        exception is YdbException ydbException &&
        (ydbException.IsTransient || (retryPolicyConfig.EnableRetryIdempotence && ydbException.Code is
            StatusCode.ClientTransportUnknown or
            StatusCode.ClientTransportUnavailable or
            StatusCode.Undetermined));

    protected override void OnRetry()
    {
        base.OnRetry();
        _retryCount++;
    }

    protected override TimeSpan? GetNextDelay(Exception lastException)
    {
        var delay = _retryPolicy.GetNextDelay((YdbException)lastException, ExceptionsEncountered.Count);
        if (delay is null)
        {
            return null;
        }

        // Close the previous attempt's span and open a new one so the next operation
        // (and the Task.Delay before it) is parented under the fresh `ydb.Try`.
        _currentTryActivity?.SetException(lastException);
        _currentTryActivity?.Dispose();
        _currentTryActivity = StartTryActivity();
        _currentTryActivity?.SetRetryAttributes(delay.Value);

        return delay;
    }

    public override TResult Execute<TState, TResult>(
        TState state,
        Func<DbContext, TState, TResult> operation,
        Func<DbContext, TState, ExecutionResult<TResult>>? verifySucceeded
    )
    {
        using var dbActivity = YdbActivitySource.StartActivity(_activityName, ActivityKind.Internal);
        var startTimestamp = YdbMetricsReporter.ReportRetryStart();
        BeginRetryLoop();
        try
        {
            var result = base.Execute(state, operation, verifySucceeded);
            EndRetryLoop(startTimestamp);
            return result;
        }
        catch (Exception e)
        {
            EndRetryLoop(startTimestamp, dbActivity, e);
            throw;
        }
    }

    public override async Task<TResult> ExecuteAsync<TState, TResult>(
        TState state,
        Func<DbContext, TState, CancellationToken, Task<TResult>> operation,
        Func<DbContext, TState, CancellationToken, Task<ExecutionResult<TResult>>>? verifySucceeded,
        CancellationToken cancellationToken = default
    )
    {
        using var dbActivity = YdbActivitySource.StartActivity(_activityName, ActivityKind.Internal);
        var startTimestamp = YdbMetricsReporter.ReportRetryStart();
        BeginRetryLoop();
        try
        {
            var result = await base.ExecuteAsync(state, operation, verifySucceeded, cancellationToken)
                .ConfigureAwait(false);
            EndRetryLoop(startTimestamp);
            return result;
        }
        catch (Exception e)
        {
            EndRetryLoop(startTimestamp, dbActivity, e);
            throw;
        }
    }

    private static Activity? StartTryActivity() =>
        YdbActivitySource.StartActivity(TryActivityName, ActivityKind.Internal);

    private void BeginRetryLoop()
    {
        _retryCount = 0;
        _currentTryActivity = StartTryActivity();
    }

    private void EndRetryLoop(long startTimestamp, Activity? dbActivity = null, Exception? exception = null)
    {
        if (exception is not null)
        {
            _currentTryActivity?.SetException(exception);
            dbActivity?.SetException(exception);
        }

        _currentTryActivity?.Dispose();
        YdbMetricsReporter.ReportRetryStop(startTimestamp, _retryCount + 1, retryPolicyConfig.OperationName);
    }
}
