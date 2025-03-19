using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbProviderFactory : DbProviderFactory
{
    public static readonly YdbProviderFactory Instance = new();

    public override YdbCommand CreateCommand() => new();

    public override YdbConnection CreateConnection() => new();

    public override YdbConnectionStringBuilder CreateConnectionStringBuilder() => new();

    public override DbParameter CreateParameter() => new YdbParameter();

#if NET7_0_OR_GREATER
    public override YdbDataSource CreateDataSource(string connectionString) => new();
#endif
}
