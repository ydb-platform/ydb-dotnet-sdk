using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado.RetryPolicy;

namespace EntityFrameworkCore.Ydb.Storage.Internal;

public class YdbExecutionStrategyFactory(ExecutionStrategyDependencies dependencies)
    : RelationalExecutionStrategyFactory(dependencies)
{
    protected override IExecutionStrategy CreateDefaultStrategy(ExecutionStrategyDependencies dependencies) =>
        new YdbExecutionStrategy(dependencies, YdbRetryPolicyConfig.Default);
}
