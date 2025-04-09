using EfCore.Ydb.Update.Internal;
using Microsoft.EntityFrameworkCore.Update;

namespace EntityFrameworkCore.Ydb.Update.Internal;

public sealed class YdbModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    : IModificationCommandBatchFactory
{
    private ModificationCommandBatchFactoryDependencies Dependencies { get; } = dependencies;

    public ModificationCommandBatch Create() => new YdbModificationCommandBatch(Dependencies);
}
