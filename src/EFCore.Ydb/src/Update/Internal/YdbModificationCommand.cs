using Microsoft.EntityFrameworkCore.Update;

namespace EntityFrameworkCore.Ydb.Update.Internal;

public class YdbModificationCommand : ModificationCommand
{
    public YdbModificationCommand(
        in ModificationCommandParameters modificationCommandParameters
    ) : base(in modificationCommandParameters)
    {
    }

    public YdbModificationCommand(
        in NonTrackedModificationCommandParameters modificationCommandParameters
    ) : base(in modificationCommandParameters)
    {
    }
}
