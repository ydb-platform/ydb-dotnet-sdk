using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using EntityFrameworkCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.Logging;
using Ydb.Sdk;
using Ydb.Sdk.Ado;

namespace EntityFrameworkCore.Ydb.Migrations.Internal;

public class YdbHistoryRepository(HistoryRepositoryDependencies dependencies)
    : HistoryRepository(dependencies), IHistoryRepository
{
    private const string LockKey = "LockMigration";
    private const int ReleaseMaxAttempt = 10;

    private static readonly TimeSpan LockTimeout = TimeSpan.FromMinutes(2);

    protected override bool InterpretExistsResult(object? value)
        => throw new InvalidOperationException("Shouldn't be called");

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => AcquireDatabaseLockAsync().GetAwaiter().GetResult();

    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default
    )
    {
        Dependencies.MigrationsLogger.AcquiringMigrationLock();

        var deadline = DateTime.UtcNow + LockTimeout;
        DateTime now;

        do
        {
            now = DateTime.UtcNow;

            try
            {
                await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(
                    AcquireDatabaseLockCommand(),
                    ((IYdbRelationalConnection)Dependencies.Connection).Clone(), // TODO usage ExecutionContext
                    new MigrationExecutionState(),
                    commitTransaction: true,
                    cancellationToken: cancellationToken
                ).ConfigureAwait(false);

                return new YdbMigrationDatabaseLock(this);
            }
            catch (YdbException)
            {
                await Task.Delay(100 + Random.Shared.Next(1000), cancellationToken);
            }
        } while (now < deadline);

        throw new YdbException("Unable to obtain table lock - another EF instance may be running");
    }

    private IReadOnlyList<MigrationCommand> AcquireDatabaseLockCommand() =>
        Dependencies.MigrationsSqlGenerator.Generate(new List<MigrationOperation>
        {
            new SqlOperation
            {
                Sql = GetInsertScript(
                    new HistoryRow(
                        LockKey,
                        $"LockTime: {DateTime.UtcNow.ToString(CultureInfo.InvariantCulture)}, PID: {Environment.ProcessId}"
                    )
                )
            }
        });

    private async Task ReleaseDatabaseLockAsync()
    {
        for (var i = 0; i < ReleaseMaxAttempt; i++)
        {
            await using var connection = ((IYdbRelationalConnection)Dependencies.Connection).Clone().DbConnection;

            try
            {
                await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(
                    ReleaseDatabaseLockCommand(),
                    ((IYdbRelationalConnection)Dependencies.Connection).Clone()
                ).ConfigureAwait(false);

                return;
            }
            catch (YdbException e)
            {
                Dependencies.MigrationsLogger.Logger.LogError(e, "Failed release database lock");
            }
        }
    }

    private IReadOnlyList<MigrationCommand> ReleaseDatabaseLockCommand() =>
        Dependencies.MigrationsSqlGenerator.Generate(new List<MigrationOperation>
            { new SqlOperation { Sql = GetDeleteScript(LockKey) } }
        );

    bool IHistoryRepository.CreateIfNotExists() => CreateIfNotExistsAsync().GetAwaiter().GetResult();

    public async Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
    {
        if (await ExistsAsync(cancellationToken))
        {
            return false;
        }

        try
        {
            await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(
                GetCreateIfNotExistsCommands(),
                Dependencies.Connection,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            return true;
        }
        catch (YdbException e)
        {
            if (e.Code == StatusCode.Overloaded)
            {
                return true;
            }

            throw;
        }
    }

    private IReadOnlyList<MigrationCommand> GetCreateIfNotExistsCommands() =>
        Dependencies.MigrationsSqlGenerator.Generate(new List<MigrationOperation>
        {
            new SqlOperation
            {
                Sql = GetCreateIfNotExistsScript(),
                SuppressTransaction = true
            }
        });

    public override string GetCreateIfNotExistsScript()
        => GetCreateScript().Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Transaction;

    protected override string ExistsSql
        => throw new UnreachableException("Shouldn't be called. We check if exists using different approach");

    public override bool Exists()
        => ExistsAsync().ConfigureAwait(false).GetAwaiter().GetResult();

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(
                SelectHistoryTableCommand(),
                Dependencies.Connection,
                new MigrationExecutionState(),
                commitTransaction: true,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            return true;
        }
        catch (YdbException)
        {
            return false;
        }
    }

    private IReadOnlyList<MigrationCommand> SelectHistoryTableCommand() =>
        Dependencies.MigrationsSqlGenerator.Generate(new List<MigrationOperation>
        {
            new SqlOperation
            {
                Sql = $"SELECT * FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)}" +
                      $" WHERE MigrationId = '{LockKey}';"
            }
        });

    public override string GetBeginIfNotExistsScript(string migrationId) => throw new NotSupportedException();

    public override string GetBeginIfExistsScript(string migrationId) => throw new NotSupportedException();

    public override string GetEndIfScript() => throw new NotSupportedException();

    private sealed class YdbMigrationDatabaseLock(YdbHistoryRepository historyRepository) : IMigrationsDatabaseLock
    {
        public void Dispose() => historyRepository.ReleaseDatabaseLockAsync().GetAwaiter().GetResult();

        public async ValueTask DisposeAsync() => await historyRepository.ReleaseDatabaseLockAsync();

        public IHistoryRepository HistoryRepository { get; } = historyRepository;
    }
}
