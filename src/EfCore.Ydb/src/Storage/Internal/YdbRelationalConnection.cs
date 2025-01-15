using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Storage.Internal;

public class YdbRelationalConnection : RelationalConnection, IYdbRelationalConnection
{
    public YdbRelationalConnection(
        RelationalConnectionDependencies dependencies
    ) : base(dependencies)
    {
    }

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
}
