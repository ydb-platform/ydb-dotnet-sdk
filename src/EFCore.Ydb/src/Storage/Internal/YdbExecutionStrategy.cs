using System;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

/// <summary>
/// Retry strategy for YDB.
///
/// IMPORTANT:
/// <br/>- The maximum number of attempts and backoff logic are encapsulated in <see cref="IRetryPolicy"/>.
/// The base ExecutionStrategy parameters (maxRetryCount, maxRetryDelay) are not used.
/// <br/>- This strategy must be invoked in the correct EF Core context/connection (YDB),
/// so that exception types and ShouldRetryOn semantics match the provider.
/// <br/>- This base <see cref="ExecutionStrategy"/> is a good place to emit metrics/logs (attempt number, delay, exception type, etc.).
/// </summary>
public class YdbExecutionStrategy(ExecutionStrategyDependencies dependencies, YdbRetryPolicyConfig retryPolicyConfig)
// We pass "placeholders" to the base class:
// - TimeSpan.Zero is not used in the real retry logic.
// - Actual limits/delays are driven by IRetryPolicy.
    : ExecutionStrategy(dependencies, retryPolicyConfig.MaxAttempts, TimeSpan.Zero /* unused! */)
{
    private readonly YdbRetryPolicy _retryPolicy = new(retryPolicyConfig);

    public override bool RetriesOnFailure => true;

    protected override bool ShouldRetryOn(Exception exception) =>
        exception is YdbException ydbException &&
        (ydbException.IsTransient || retryPolicyConfig.EnableRetryIdempotence && ydbException.Code is
            StatusCode.ClientTransportUnknown or
            StatusCode.ClientTransportUnavailable or
            StatusCode.Undetermined);

    protected override TimeSpan? GetNextDelay(Exception lastException) =>
        _retryPolicy.GetNextDelay((YdbException)lastException, ExceptionsEncountered.Count - 1);
}
