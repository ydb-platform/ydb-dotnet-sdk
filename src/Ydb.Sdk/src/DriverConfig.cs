using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk;

public class DriverConfig
{
    private readonly string _pid = Environment.ProcessId.ToString();

    public string Endpoint { get; }
    public string Database { get; }
    public ICredentialsProvider? Credentials { get; }

    public TimeSpan ConnectTimeout { get; init; } =
        TimeSpan.FromSeconds(GrpcDefaultSettings.ConnectTimeoutSeconds);

    public TimeSpan KeepAlivePingDelay { get; init; } =
        TimeSpan.FromSeconds(GrpcDefaultSettings.KeepAlivePingSeconds);

    public TimeSpan KeepAlivePingTimeout { get; init; } =
        TimeSpan.FromSeconds(GrpcDefaultSettings.KeepAlivePingTimeoutSeconds);

    public string? User { get; init; }
    public string? Password { get; init; }

    public bool EnableMultipleHttp2Connections { get; init; } = GrpcDefaultSettings.EnableMultipleHttp2Connections;

    public int MaxSendMessageSize { get; init; } = GrpcDefaultSettings.MaxSendMessageSize;
    public int MaxReceiveMessageSize { get; init; } = GrpcDefaultSettings.MaxReceiveMessageSize;

    internal X509Certificate2Collection CustomServerCertificates { get; } = new();
    internal TimeSpan EndpointDiscoveryInterval = TimeSpan.FromMinutes(1);
    internal TimeSpan EndpointDiscoveryTimeout = TimeSpan.FromSeconds(10);
    internal string SdkVersion { get; }

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
