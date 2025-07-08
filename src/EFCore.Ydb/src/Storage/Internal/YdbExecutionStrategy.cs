using System;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public class YdbExecutionStrategy(ExecutionStrategyDependencies dependencies)
    : ExecutionStrategy(dependencies, maxRetryCount: 10, maxRetryDelay: TimeSpan.FromSeconds(10)) // TODO User settings
{
    protected override bool ShouldRetryOn(Exception exception)
        => exception is YdbException { IsTransient: true };
}
