using Microsoft.EntityFrameworkCore.Update;

namespace EntityFrameworkCore.Ydb.Update.Internal;

public class YdbModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies
) : AffectedCountModificationCommandBatch(dependencies, 100 /* Temporary solve */);
