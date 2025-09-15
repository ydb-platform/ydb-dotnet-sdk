using Ydb.Sdk.Ado.RetryPolicy;

namespace Ydb.Sdk.Ado;

public class YdbDataSourceBuilder
{
    public YdbDataSourceBuilder()
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder();
    }

    public YdbDataSourceBuilder(string connectionString)
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    public YdbDataSourceBuilder(YdbConnectionStringBuilder connectionStringBuilder)
    {
        ConnectionStringBuilder = connectionStringBuilder;
    }

    /// <summary>
    /// A connection string builder that can be used to configure the connection string on the builder.
    /// </summary>
    public YdbConnectionStringBuilder ConnectionStringBuilder { get; }

    /// <summary>
    /// Returns the connection string, as currently configured on the builder.
    /// </summary>
    public string ConnectionString => ConnectionStringBuilder.ConnectionString;

    public IRetryPolicy RetryPolicy { get; set; } = new YdbRetryPolicy(YdbRetryPolicyConfig.Default);

    public YdbDataSource Build() => new(this);
}
