using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EfCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Migrations.Internal;

public class YdbHistoryRepository : HistoryRepository
{
    public YdbHistoryRepository(HistoryRepositoryDependencies dependencies) : base(dependencies)
    {
    }

    protected override bool InterpretExistsResult(object? value)
        => throw new InvalidOperationException("Shouldn't be called");

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
    {
        Dependencies.MigrationsLogger.AcquiringMigrationLock();
        return new YdbMigrationDatabaseLock(this, Dependencies.Connection);
    }

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default
    )
    {
        throw new NotImplementedException();
    }

    public override string GetCreateIfNotExistsScript()
        => GetCreateScript().Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Transaction;

    protected override string ExistsSql
        => throw new UnreachableException("Shouldn't be called. We check if exists using different approach");

    public override bool Exists()
        => ExistsAsync().ConfigureAwait(false).GetAwaiter().GetResult();

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        var connection = (YdbRelationalConnection)Dependencies.Connection;
        var schema = (YdbConnection)connection.DbConnection;
        var tables = schema.GetSchema("tables");

        var foundTables =
            from table in tables.AsEnumerable()
            where table.Field<string>("table_type") == "TABLE"
                  && table.Field<string>("table_name") == TableName
            select table;
        return Task.FromResult(foundTables.Count() == 1);
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

    // TODO Implement lock
    private sealed class YdbMigrationDatabaseLock : IMigrationsDatabaseLock
    {
        private YdbRelationalConnection _connection;

        public YdbMigrationDatabaseLock(
            IHistoryRepository historyRepository,
            IRelationalConnection connection
        )
        {
            HistoryRepository = historyRepository;
            _connection = (YdbRelationalConnection)connection;
        }

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync()
            => default;

        public IHistoryRepository HistoryRepository { get; }
    }
}
