using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EfCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Migrations.Internal;

// ReSharper disable once ClassNeverInstantiated.Global
public class YdbHistoryRepository(HistoryRepositoryDependencies dependencies) : HistoryRepository(dependencies)
{
    protected override bool InterpretExistsResult(object? value)
        => throw new InvalidOperationException("Shouldn't be called");

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => AcquireDatabaseLockAsync().GetAwaiter().GetResult();

    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default
    )
    {
        Dependencies.MigrationsLogger.AcquiringMigrationLock();
        var dbLock =
            new YdbMigrationDatabaseLock("migrationLock", this, (YdbRelationalConnection)Dependencies.Connection);
        await dbLock.Lock(timeoutInSeconds: 60, cancellationToken);
        return dbLock;
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

    private sealed class YdbMigrationDatabaseLock(
        string name,
        IHistoryRepository historyRepository,
        YdbRelationalConnection ydbConnection
    ) : IMigrationsDatabaseLock
    {
        private IYdbRelationalConnection Connection { get; } = ydbConnection.Clone();
        private volatile string _pid = null!;
        private CancellationTokenSource? _watchDogToken;

        public async Task Lock(int timeoutInSeconds, CancellationToken cancellationToken = default)
        {
            if (_watchDogToken != null)
            {
                throw new InvalidOperationException("Already locked");
            }

            await Connection.OpenAsync(cancellationToken);
            await using (var command = Connection.DbConnection.CreateCommand())
            {
                command.CommandText = """
                                      CREATE TABLE IF NOT EXISTS shedlock (
                                          name Text NOT NULL,
                                          locked_at Timestamp NOT NULL,
                                          lock_until Timestamp NOT NULL,
                                          locked_by Text NOT NULL,
                                          PRIMARY KEY(name)
                                      );
                                      """;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            _pid = $"PID:{Environment.ProcessId}";

            var lockAcquired = false;
            for (var i = 0; i < 10; i++)
            {
                if (await UpdateLock(name, timeoutInSeconds))
                {
                    lockAcquired = true;
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }

            if (!lockAcquired)
            {
                throw new TimeoutException("Failed to acquire lock for migration`");
            }

            _watchDogToken = new CancellationTokenSource();
            _ = Task.Run((async Task () =>
            {
                while (true)
                {
                    // ReSharper disable once PossibleLossOfFraction
                    await Task.Delay(TimeSpan.FromSeconds(timeoutInSeconds / 2), _watchDogToken.Token);
                    await UpdateLock(name, timeoutInSeconds);
                }
            })!, _watchDogToken.Token);
        }

        private async Task<bool> UpdateLock(string name, int timeoutInSeconds)
        {
            var command = Connection.DbConnection.CreateCommand();
            command.CommandText =
                $"""
                 UPSERT INTO shedlock (name, locked_at, lock_until, locked_by)
                 VALUES (
                        @name,
                        CurrentUtcTimestamp(), 
                        Unwrap(CurrentUtcTimestamp() + Interval("PT{timeoutInSeconds}S")),
                        @locked_by
                        );
                 """;
            command.Parameters.Add(new YdbParameter("name", DbType.String, name));
            command.Parameters.Add(new YdbParameter("locked_by", DbType.String, _pid));

            try
            {
                await command.ExecuteNonQueryAsync();
                return true;
            }
            catch (YdbException)
            {
                return false;
            }
        }

        public void Dispose()
            => DisposeInternalAsync().GetAwaiter().GetResult();

        public async ValueTask DisposeAsync()
            => await DisposeInternalAsync();

        private async Task DisposeInternalAsync()
        {
            if (_watchDogToken != null)
            {
                await _watchDogToken.CancelAsync();
            }

            _watchDogToken = null;
            await using var connection = Connection.DbConnection.CreateCommand();
            connection.CommandText = "DELETE FROM shedlock WHERE name = '{_name}' AND locked_by = '{PID}';";
            await connection.ExecuteNonQueryAsync();
        }

        public IHistoryRepository HistoryRepository { get; } = historyRepository;
    }
}
