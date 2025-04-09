using EfCore.Ydb.Update.Internal;
using Microsoft.EntityFrameworkCore.Update;

namespace EntityFrameworkCore.Ydb.Update.Internal;

public class YdbModificationCommandFactory : IModificationCommandFactory
{
    public IModificationCommand CreateModificationCommand(
        in ModificationCommandParameters modificationCommandParameters
    ) => new YdbModificationCommand(modificationCommandParameters);

    public INonTrackedModificationCommand CreateNonTrackedModificationCommand(
        in NonTrackedModificationCommandParameters modificationCommandParameters
    ) => new YdbModificationCommand(modificationCommandParameters);
}
