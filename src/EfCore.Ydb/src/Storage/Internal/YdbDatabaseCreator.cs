using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Storage.Internal;

public class YdbDatabaseCreator(
    RelationalDatabaseCreatorDependencies dependencies,
    IYdbRelationalConnection connection
) : RelationalDatabaseCreator(dependencies)
{
    public override bool Exists()
        => ExistsInternal().GetAwaiter().GetResult();

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = new())
        => ExistsInternal(cancellationToken);

    private async Task<bool> ExistsInternal(CancellationToken cancellationToken = default)
    {
        var connection1 = connection.Clone();
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
            await connection1.CloseAsync().ConfigureAwait(false);
            await connection1.DisposeAsync().ConfigureAwait(false);
        }
    }

    // TODO: Implement later
    public override bool HasTables() => false;

    public override void Create() => throw new NotSupportedException("YDB does not support database creation");

    public override void Delete() => throw new NotSupportedException("YDB does not support database deletion");
}
