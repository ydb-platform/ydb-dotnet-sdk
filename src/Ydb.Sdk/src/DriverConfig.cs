using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk;

/// <summary>
/// Configuration settings for YDB driver.
/// </summary>
/// <remarks>
/// DriverConfig contains all the necessary settings to configure a YDB driver instance,
/// including connection parameters, timeouts, and authentication credentials.
/// </remarks>
public class DriverConfig
{
    private readonly string _pid = Environment.ProcessId.ToString();

    /// <summary>
    /// Gets the YDB server endpoint URL.
    /// </summary>
    public string Endpoint { get; }
    
    /// <summary>
    /// Gets the database path.
    /// </summary>
    public string Database { get; }
    
    /// <summary>
    /// Gets the credentials provider for authentication.
    /// </summary>
    public ICredentialsProvider? Credentials { get; }

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; init; } =
        TimeSpan.FromSeconds(GrpcDefaultSettings.ConnectTimeoutSeconds);

    /// <summary>
    /// Gets or sets the keep-alive ping delay.
    /// </summary>
    public TimeSpan KeepAlivePingDelay { get; init; } =
        TimeSpan.FromSeconds(GrpcDefaultSettings.KeepAlivePingSeconds);

    /// <summary>
    /// Gets or sets the keep-alive ping timeout.
    /// </summary>
    public TimeSpan KeepAlivePingTimeout { get; init; } =
        TimeSpan.FromSeconds(GrpcDefaultSettings.KeepAlivePingTimeoutSeconds);

    /// <summary>
    /// Gets or sets the username for basic authentication.
    /// </summary>
    public string? User { get; init; }
    
    /// <summary>
    /// Gets or sets the password for basic authentication.
    /// </summary>
    public string? Password { get; init; }

    /// <summary>
    /// Gets or sets a value indicating whether to enable multiple HTTP/2 connections.
    /// </summary>
    public bool EnableMultipleHttp2Connections { get; init; } = GrpcDefaultSettings.EnableMultipleHttp2Connections;

    /// <summary>
    /// Gets or sets the maximum send message size in bytes.
    /// </summary>
    public int MaxSendMessageSize { get; init; } = GrpcDefaultSettings.MaxSendMessageSize;
    
    /// <summary>
    /// Gets or sets the maximum receive message size in bytes.
    /// </summary>
    public int MaxReceiveMessageSize { get; init; } = GrpcDefaultSettings.MaxReceiveMessageSize;

    internal X509Certificate2Collection CustomServerCertificates { get; } = new();
    internal TimeSpan EndpointDiscoveryInterval = TimeSpan.FromMinutes(1);
    internal TimeSpan EndpointDiscoveryTimeout = TimeSpan.FromSeconds(10);
    internal string SdkVersion { get; }

    /// <summary>
    /// Initializes a new instance of the DriverConfig class.
    /// </summary>
    /// <param name="endpoint">The YDB server endpoint URL.</param>
    /// <param name="database">The database path.</param>
    /// <param name="credentials">Optional credentials provider for authentication.</param>
    /// <param name="customServerCertificate">Optional custom server certificate for TLS validation.</param>
    /// <param name="customServerCertificates">Optional collection of custom server certificates for TLS validation.</param>
    public DriverConfig(
        string endpoint,
        string database,
        ICredentialsProvider? credentials = null,
        X509Certificate? customServerCertificate = null,
        X509Certificate2Collection? customServerCertificates = null)
    {
        Endpoint = FormatEndpoint(endpoint);
        Database = database;
        Credentials = credentials;

        if (customServerCertificate != null)
        {
            CustomServerCertificates.Add(new X509Certificate2(customServerCertificate));
        }

        if (customServerCertificates != null)
        {
            CustomServerCertificates.AddRange(customServerCertificates);
        }

        var version = Assembly.GetExecutingAssembly().GetName().Version;
        var versionStr = version is null ? "unknown" : version.ToString(3);
        SdkVersion = $"ydb-dotnet-sdk/{versionStr}";
    }

    internal Grpc.Core.Metadata GetCallMetadata => new()
    {
        { Metadata.RpcDatabaseHeader, Database },
        { Metadata.RpcSdkInfoHeader, SdkVersion },
        { Metadata.RpcClientPid, _pid }
    };

    private static string FormatEndpoint(string endpoint)
    {
        endpoint = endpoint.ToLower().Trim();

        if (endpoint.StartsWith("http://") || endpoint.StartsWith("https://"))
        {
            return endpoint;
        }

        if (endpoint.StartsWith("grpc://"))
        {
            var builder = new UriBuilder(endpoint) { Scheme = Uri.UriSchemeHttp };
            return builder.Uri.ToString();
        }

        if (endpoint.StartsWith("grpcs://"))
        {
            var builder = new UriBuilder(endpoint) { Scheme = Uri.UriSchemeHttps };
            return builder.Uri.ToString();
        }

        return $"https://{endpoint}";
    }
}
