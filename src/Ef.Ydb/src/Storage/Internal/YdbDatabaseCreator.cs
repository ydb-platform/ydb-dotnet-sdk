using System;
using Microsoft.EntityFrameworkCore.Storage;

namespace Ef.Ydb.Storage.Internal;

public class YdbDatabaseCreator : RelationalDatabaseCreator
{
    public YdbDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies
    ) : base(dependencies)
    {
    }

    public override bool Exists()
    {
        throw new NotImplementedException();
    }

    public override bool HasTables()
    {
        throw new NotImplementedException();
    }

    public override void Create()
    {
        throw new NotImplementedException();
    }

    public override void Delete()
    {
        throw new NotImplementedException();
    }
}
