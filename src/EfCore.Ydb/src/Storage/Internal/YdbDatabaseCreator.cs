using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Storage.Internal;

public class YdbDatabaseCreator(
    RelationalDatabaseCreatorDependencies dependencies
) : RelationalDatabaseCreator(dependencies)
{
    public override bool Exists()
        => ExistsInternal().GetAwaiter().GetResult();

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = new())
        => ExistsInternal(cancellationToken);

    private async Task<bool> ExistsInternal(CancellationToken cancellationToken = default)
    {
        await using var connection = Dependencies.Connection;
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

    public override bool HasTables() => false;

    public override void Create() => throw new NotSupportedException("YDB does not support database creation");

    public override void Delete() => throw new NotSupportedException("YDB does not support database deletion");
}
