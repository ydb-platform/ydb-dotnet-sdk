using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Storage.Internal;

public class YdbDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IYdbRelationalConnection _connection;
    private readonly IRelationalConnectionDiagnosticsLogger _connectionLogger;

    public YdbDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IYdbRelationalConnection connection,
        IRawSqlCommandBuilder rawSqlCommandBuilder,
        IRelationalConnectionDiagnosticsLogger connectionLogger
    ) : base(dependencies)
    {
        _connection = connection;
        _connectionLogger = connectionLogger;
    }

    public override bool Exists()
        => ExistsInternal().GetAwaiter().GetResult();

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = new CancellationToken())
        => ExistsInternal(cancellationToken);

    private async Task<bool> ExistsInternal(CancellationToken cancellationToken = default)
    {
        var connection = _connection.Clone();
        try
        {
            await connection.OpenAsync(cancellationToken, errorsExpected: true);
            return true;
        }
        catch (YdbException)
        {
            return false;
        }
        finally
        {
            await connection.CloseAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    public override bool HasTables()
    {
        throw new NotImplementedException();
    }

    public override void Create()
        => throw new NotSupportedException("Ydb does not support database creation");

    public override void Delete()
        => throw new NotSupportedException("Ydb does not support database deletion");
}
