using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Ef.Ydb.Migrations.Internal;

// TODO: For migrations only. Not required for mvp
public class YdbHistoryRepository : HistoryRepository
{
    public YdbHistoryRepository(
        HistoryRepositoryDependencies dependencies
    ) : base(dependencies)
    {
    }

    protected override bool InterpretExistsResult(object? value)
    {
        throw new NotImplementedException();
    }

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        throw new NotImplementedException();
    }

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = new CancellationToken())
    {
        throw new NotImplementedException();
    }

    public override string GetCreateIfNotExistsScript()
    {
        throw new NotImplementedException();
    }

    public override string GetBeginIfNotExistsScript(string migrationId)
    {
        throw new NotImplementedException();
    }

    public override string GetBeginIfExistsScript(string migrationId)
    {
        throw new NotImplementedException();
    }

    public override string GetEndIfScript()
    {
        throw new NotImplementedException();
    }

    public override LockReleaseBehavior LockReleaseBehavior { get; }
    protected override string ExistsSql { get; }
}
