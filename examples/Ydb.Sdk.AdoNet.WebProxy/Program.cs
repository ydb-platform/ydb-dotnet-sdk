using System.Net;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;

using var loggerFactory = LoggerFactory.Create(builder =>
    builder.AddConsole().SetMinimumLevel(LogLevel.Information));
var logger = loggerFactory.CreateLogger("WebProxyExample");

// Linux CI: Squid with --network host → YDB is localhost.
// Docker Desktop (macOS/Windows): Squid is bridged → YDB via host.docker.internal.
var ydbHost = OperatingSystem.IsLinux() ? "localhost" : "host.docker.internal";

var proxy = new WebProxy(new Uri("http://127.0.0.1:3128"))
{
    Credentials = new NetworkCredential("proxy-user", "proxy-pass")
};

logger.LogInformation("Connecting to YDB ({Host}) via HTTP proxy {Proxy}", ydbHost, proxy.Address);

// Cleartext HTTP/2 through CONNECT does not work in .NET even with
// Http2UnencryptedSupport — use TLS (2135) + local YDB CA.
var caPath = Path.Combine(
    Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
    "ca.pem");

await using var dataSource = new YdbDataSource(new YdbConnectionStringBuilder
{
    Host = ydbHost,
    Port = 2135,
    Database = "/local",
    UseTls = true,
    RootCertificate = caPath,
    DisableDiscovery = true,
    Proxy = proxy,
    LoggerFactory = loggerFactory
});

await using var connection = await dataSource.OpenConnectionAsync();
await using var command = connection.CreateCommand();
command.CommandText = "SELECT 'Hello from YDB through HTTP proxy!'u AS message, CurrentUtcTimestamp() AS ts";

await using var reader = await command.ExecuteReaderAsync();
if (!await reader.ReadAsync())
{
    throw new InvalidOperationException("Query returned no rows");
}

logger.LogInformation("message={Message}, ts={Timestamp}",
    reader.GetString(0),
    reader.GetValue(1));

logger.LogInformation("Connection through proxy succeeded");
