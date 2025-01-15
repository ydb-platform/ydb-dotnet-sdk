using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public class YdbModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public YdbModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies
    ) => Dependencies = dependencies;

    protected virtual ModificationCommandBatchFactoryDependencies Dependencies { get; }

    public ModificationCommandBatch Create()
        => new TemporaryStubModificationCommandBatch(Dependencies);
}
// TODO: replace with more flexible realisation
class TemporaryStubModificationCommandBatch : AffectedCountModificationCommandBatch
{
    public TemporaryStubModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies
    ) : base(dependencies, 1)
    {
    }
}
