using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public sealed class YdbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    private ModificationCommandBatchFactoryDependencies Dependencies { get; } = dependencies;

    public ModificationCommandBatch Create()
        => new MyModificationCommandBatch(Dependencies);
}

internal class MyModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies,
    int? maxBatchSize = MyModificationCommandBatch.CustomMaxBatchSize
) : AffectedCountModificationCommandBatch(dependencies, maxBatchSize)
{
    public const int CustomMaxBatchSize = 4096;
}
