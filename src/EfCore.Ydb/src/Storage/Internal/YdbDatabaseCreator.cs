using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Storage.Internal;

public class YdbDatabaseCreator(RelationalDatabaseCreatorDependencies dependencies)
    : RelationalDatabaseCreator(dependencies)
{
    public override bool Exists() => ExistsInternal().GetAwaiter().GetResult();

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default) =>
        ExistsInternal(cancellationToken);

    private async Task<bool> ExistsInternal(CancellationToken cancellationToken = default)
    {
        await using var connection = ((IYdbRelationalConnection)Dependencies.Connection).Clone();

        try
        {
            await connection.OpenAsync(cancellationToken, errorsExpected: true);
            return true;
        }
        catch (YdbException)
        {
            return false;
        }
    }

    public override bool HasTables() => HasTablesAsync().GetAwaiter().GetResult();

    public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = ((IYdbRelationalConnection)Dependencies.Connection).Clone();

        try
        {
            await connection.OpenAsync(cancellationToken, errorsExpected: true);

            var dataTable = await connection
                .DbConnection
                .GetSchemaAsync("Tables", [null, "TABLE"], cancellationToken);

            return dataTable.Rows.Count > 0;
        }
        catch (YdbException)
        {
            return false;
        }
    }

    public override void Create() => CreateAsync().GetAwaiter().GetResult();

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        if (await ExistsAsync(cancellationToken))
        {
            return;
        }

        throw new NotSupportedException("YDB does not support database creation");
    }

    public override void Delete() => DeleteAsync().GetAwaiter().GetResult();

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = Dependencies.Connection;
        await connection.OpenAsync(cancellationToken, errorsExpected: true);

        var dataTable = await connection
            .DbConnection
            .GetSchemaAsync("Tables", [null, "TABLE"], cancellationToken);

        var dropTableOperations = (from DataRow row in dataTable.Rows
            select new DropTableOperation { Name = row["table_name"].ToString() }).ToList();

        await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(Dependencies.MigrationsSqlGenerator
            .Generate(dropTableOperations), connection, cancellationToken);
    }
}
