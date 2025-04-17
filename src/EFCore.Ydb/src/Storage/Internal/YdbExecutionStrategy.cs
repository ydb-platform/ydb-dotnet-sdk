using System;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public class YdbExecutionStrategy(ExecutionStrategyDependencies dependencies)
    : ExecutionStrategy(dependencies, maxRetryCount: 10, maxRetryDelay: TimeSpan.FromSeconds(10))
{
    protected override bool ShouldRetryOn(Exception exception)
        => exception is YdbException ydbException && (
            TransactionLockInvalidated(ydbException)
            || false // For other possible exceptions
        );

    private static bool TransactionLockInvalidated(YdbException exception)
        => exception.Message.Contains("Transaction locks invalidated");
}
