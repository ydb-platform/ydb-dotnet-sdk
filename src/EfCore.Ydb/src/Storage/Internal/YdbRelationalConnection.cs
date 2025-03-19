using System.Data.Common;
using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Storage.Internal;

public class YdbRelationalConnection(RelationalConnectionDependencies dependencies)
    : RelationalConnection(dependencies), IYdbRelationalConnection
{
    public DbDataSource? DataSource { get; private set; }

    protected override DbConnection CreateDbConnection()
    {
        if (DataSource is not null)
        {
            return DataSource.CreateConnection();
        }

        var connection = new YdbConnection(GetValidatedConnectionString());
        return connection;
    }

    public IYdbRelationalConnection Clone()
    {
        var connectionStringBuilder = new YdbConnectionStringBuilder(GetValidatedConnectionString());
        var options = new DbContextOptionsBuilder().UseEfYdb(connectionStringBuilder.ToString()).Options;
        return new YdbRelationalConnection(Dependencies with { ContextOptions = options });
    }
}
