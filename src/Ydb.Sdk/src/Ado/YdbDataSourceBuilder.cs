using Ydb.Sdk.Ado.RetryPolicy;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Provides a simple way to create and configure a <see cref="YdbDataSource"/>.
/// </summary>
/// <remarks>
/// YdbDataSourceBuilder provides a fluent interface for configuring connection strings
/// and retry policies before building a YdbDataSource instance. It supports both
/// string-based and strongly-typed connection string configuration.
///
/// <para>
/// For more information about YDB, see:
/// <see href="https://ydb.tech/docs">YDB Documentation</see>.
/// </para>
/// </remarks>
public class YdbDataSourceBuilder
{
    /// <summary>
    /// Initializes a new instance of the <see cref="YdbDataSourceBuilder"/> class with default settings.
    /// </summary>
    /// <remarks>
    /// Creates a new builder with default connection string and retry policy settings.
    /// </remarks>
    public YdbDataSourceBuilder()
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbDataSourceBuilder"/> class with the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to use for the data source.</param>
    /// <remarks>
    /// Creates a new builder with the specified connection string and default retry policy.
    /// </remarks>
    public YdbDataSourceBuilder(string connectionString)
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbDataSourceBuilder"/> class with the specified connection string builder.
    /// </summary>
    /// <param name="connectionStringBuilder">The connection string builder to use for the data source.</param>
    /// <remarks>
    /// Creates a new builder with the specified connection string builder and default retry policy.
    /// </remarks>
    public YdbDataSourceBuilder(YdbConnectionStringBuilder connectionStringBuilder)
    {
        ConnectionStringBuilder = connectionStringBuilder;
    }

    /// <summary>
    /// Gets the connection string builder that can be used to configure the connection string.
    /// </summary>
    /// <remarks>
    /// Provides strongly-typed properties for configuring YDB connection parameters.
    /// Changes to this builder will be reflected in the built data source.
    /// </remarks>
    public YdbConnectionStringBuilder ConnectionStringBuilder { get; }

    /// <summary>
    /// Gets the connection string as currently configured on the builder.
    /// </summary>
    /// <remarks>
    /// Returns the current connection string based on the configuration in the ConnectionStringBuilder.
    /// </remarks>
    public string ConnectionString => ConnectionStringBuilder.ConnectionString;

    /// <summary>
    /// Gets or sets the default retry policy for the data source.
    /// </summary>
    /// <remarks>
    /// Specifies the retry policy to use for handling transient failures.
    /// <para>Default value: <see cref="YdbRetryPolicy"/> with default configuration <see cref="YdbRetryPolicyConfig.Default"/>.</para>
    /// </remarks>
    public IRetryPolicy RetryPolicy { get; set; } = new YdbRetryPolicy(YdbRetryPolicyConfig.Default);

    /// <summary>
    /// Builds a new <see cref="YdbDataSource"/> instance with the current configuration.
    /// </summary>
    /// <returns>A new YdbDataSource instance configured with the current settings.</returns>
    /// <remarks>
    /// Creates a new data source with the configured connection string and retry policy.
    /// The builder can be reused to create multiple data sources with different configurations.
    /// </remarks>
    public YdbDataSource Build() => new(this);
}
