using System.Diagnostics;
using Ydb.Sdk.Tracing;

namespace Ydb.Sdk.Ado.RetryPolicy;

internal sealed class YdbRetryPolicyExecutor
{
    private readonly IRetryPolicy _retryPolicy;

    public YdbRetryPolicyExecutor(IRetryPolicy retryPolicy)
    {
        _retryPolicy = retryPolicy;
    }

    /// <summary>
    /// Executes the specified asynchronous operation and returns the result.
    /// </summary>
    /// <param name="operation">
    /// A function that returns a started task of type <typeparamref name="TResult" />.
    /// </param>
    /// <param name="cancellationToken">
    /// A cancellation token used to cancel the retry operation, but not operations that are already in flight
    /// or that already completed successfully.
    /// </param>
    /// <typeparam name="TResult"> The result type of the <see cref="Task{TResult}" /> returned by <paramref name="operation" />. </typeparam>
    /// <returns>
    /// A task that will run to completion if the original task completes successfully (either the
    /// first time or after retrying transient failures). If the task fails with a non-transient error or
    /// the retry limit is reached, the returned task will become faulted and the exception must be observed.
    /// </returns>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default
    ) => ExecuteImplementationAsync(operation, cancellationToken);

    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default
    ) => await ExecuteImplementationAsync(async ct =>
    {
        await operation(ct).ConfigureAwait(false);
        return 0;
    }, cancellationToken).ConfigureAwait(false);

    private async Task<TResult> ExecuteImplementationAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken
    )
    {
        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var dbActive = YdbActivitySource.StartActivity("ydb.RetryCtx.Execute", ActivityKind.Internal);

            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (YdbException e)
            {
                dbActive?.SetException(e);

                var delay = _retryPolicy.GetNextDelay(e, attempt++);
                if (delay == null)
                    throw;

                dbActive?.SetRetryAttributes(delay.Value, attempt);
                await Task.Delay(delay.Value, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
