using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public class YdbModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies,
    int? maxBatchSize = null
) : AffectedCountModificationCommandBatch(dependencies, maxBatchSize);
