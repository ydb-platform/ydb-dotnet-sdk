using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbProviderFactory : DbProviderFactory
{
    public static readonly YdbProviderFactory Instance = new();

    public override YdbCommand CreateCommand()
    {
        return new YdbCommand();
    }

    public override YdbConnection CreateConnection()
    {
        return new YdbConnection();
    }

    public override YdbConnectionStringBuilder CreateConnectionStringBuilder()
    {
        return new YdbConnectionStringBuilder();
    }

    public override DbParameter CreateParameter()
    {
        return new YdbParameter();
    }

#if NET7_0_OR_GREATER
    public override YdbDataSource CreateDataSource(string connectionString)
    {
        return new YdbDataSource();
    }
#endif
}
