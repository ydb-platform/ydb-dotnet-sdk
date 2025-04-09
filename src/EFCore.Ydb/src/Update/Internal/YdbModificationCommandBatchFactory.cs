using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public sealed class YdbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    private ModificationCommandBatchFactoryDependencies Dependencies { get; } = dependencies;

    public ModificationCommandBatch Create() => new YdbModificationCommandBatch(Dependencies);
}
