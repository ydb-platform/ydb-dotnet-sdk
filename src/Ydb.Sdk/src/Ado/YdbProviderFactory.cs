using System.Data.Common;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Represents a set of methods for creating instances of a provider's implementation of the data source classes.
/// </summary>
/// <remarks>
/// YdbProviderFactory is the factory class for creating YDB-specific data source objects.
/// It provides methods to create connections, commands, parameters, and other ADO.NET objects
/// that are specific to the YDB database provider.
/// 
/// <para>
/// For more information about YDB, see:
/// <see href="https://ydb.tech/docs">YDB Documentation</see>.
/// </para>
/// </remarks>
public class YdbProviderFactory : DbProviderFactory
{
    /// <summary>
    /// Gets the singleton instance of the YdbProviderFactory.
    /// </summary>
    /// <remarks>
    /// This static instance can be used to create YDB-specific ADO.NET objects
    /// without instantiating the factory class directly.
    /// </remarks>
    public static readonly YdbProviderFactory Instance = new();

    /// <summary>
    /// Returns a strongly typed <see cref="YdbCommand"/> object.
    /// </summary>
    /// <returns>A new instance of <see cref="YdbCommand"/>.</returns>
    /// <remarks>
    /// Creates a new YDB command object that can be used to execute SQL statements
    /// and stored procedures against a YDB database.
    /// </remarks>
    public override YdbCommand CreateCommand() => new();

    /// <summary>
    /// Returns a strongly typed <see cref="YdbConnection"/> object.
    /// </summary>
    /// <returns>A new instance of <see cref="YdbConnection"/>.</returns>
    /// <remarks>
    /// Creates a new YDB connection object that can be used to connect to a YDB database.
    /// The connection must be opened before it can be used for database operations.
    /// </remarks>
    public override YdbConnection CreateConnection() => new();

    /// <summary>
    /// Returns a strongly typed <see cref="YdbConnectionStringBuilder"/> object.
    /// </summary>
    /// <returns>A new instance of <see cref="YdbConnectionStringBuilder"/>.</returns>
    /// <remarks>
    /// Creates a new YDB connection string builder that provides strongly-typed properties
    /// for building YDB connection strings with validation and IntelliSense support.
    /// </remarks>
    public override YdbConnectionStringBuilder CreateConnectionStringBuilder() => new();

    /// <summary>
    /// Returns a strongly typed <see cref="YdbParameter"/> object.
    /// </summary>
    /// <returns>A new instance of <see cref="YdbParameter"/>.</returns>
    /// <remarks>
    /// Creates a new YDB parameter object that can be used to pass parameters
    /// to YDB commands. The parameter supports YDB-specific data types and features.
    /// </remarks>
    public override DbParameter CreateParameter() => new YdbParameter();

#if NET7_0_OR_GREATER
    /// <summary>
    /// Returns a strongly typed <see cref="YdbDataSource"/> object.
    /// </summary>
    /// <param name="connectionString">The connection string to use for the data source.</param>
    /// <returns>A new instance of <see cref="YdbDataSource"/> with the specified connection string.</returns>
    /// <remarks>
    /// Creates a new YDB data source object that provides a modern, lightweight way to work with YDB.
    /// The data source is available only in .NET 7.0 and later versions.
    /// </remarks>
    public override YdbDataSource CreateDataSource(string connectionString) => new();
#endif
}
