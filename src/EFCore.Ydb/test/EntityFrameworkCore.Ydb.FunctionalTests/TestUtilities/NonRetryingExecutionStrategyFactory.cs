using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

internal class NonRetryingExecutionStrategyFactory(ExecutionStrategyDependencies dependencies)
    : RelationalExecutionStrategyFactory(dependencies)
{
    protected override IExecutionStrategy CreateDefaultStrategy(ExecutionStrategyDependencies dependencies)
        => new NonRetryingExecutionStrategy(dependencies);
}
