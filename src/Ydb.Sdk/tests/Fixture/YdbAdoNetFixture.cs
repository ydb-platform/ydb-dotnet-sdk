using AdoNet.Specification.Tests;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Tests.Ado.Specification;

namespace Ydb.Sdk.Tests.Fixture;

public class YdbAdoNetFixture : DbFactoryTestBase<YdbFactoryFixture>
{
    protected YdbAdoNetFixture(YdbFactoryFixture fixture) : base(fixture)
    {
    }

    protected override YdbConnection CreateConnection()
    {
        return (YdbConnection)base.CreateConnection();
    }

    protected override YdbConnection CreateOpenConnection()
    {
        return (YdbConnection)base.CreateOpenConnection();
    }

    protected async Task<YdbConnection> CreateOpenConnectionAsync()
    {
        var connection = new YdbConnection(new YdbConnectionStringBuilder(ConnectionString)
            { LoggerFactory = Utils.GetLoggerFactory() });
        await connection.OpenAsync();
        return connection;
    }
}
