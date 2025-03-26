using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample : TableExampleBase
{
    private BasicExample(TableClient client, string database, string path)
        : base(client, database, path)
    {
    }

    public static async Task Run(
        string endpoint,
        string database,
        ICredentialsProvider? credentialsProvider,
        X509Certificate? customServerCertificate,
        string path,
        ILoggerFactory loggerFactory)
    {
        var config = new DriverConfig(
            endpoint: endpoint,
            database: database,
            credentials: credentialsProvider,
            customServerCertificate: customServerCertificate
        );

        await using var driver = await Driver.CreateInitialized(
            config: config,
            loggerFactory: loggerFactory
        );

        using var tableClient = new TableClient(driver, new TableClientConfig());

        var example = new BasicExample(tableClient, database, path);

        await example.SchemeQuery();
        await example.FillData();
        await example.SimpleSelect(1);
        await example.SimpleUpsert(10, "Coming soon", DateTime.UtcNow);
        await example.SimpleSelect(10);
        await example.InteractiveTx();
        await example.ReadTable();
        await example.ScanQuery(DateTime.Parse("2007-01-01"));
    }

    private static ExecuteDataQuerySettings DefaultDataQuerySettings =>
        new()
        {
            // Indicates that client is no longer interested in the result of operation after the
            // specified duration starting from the moment when operation arrives at the server.
            // Status code TIMEOUT will be returned from server in case when operation result in
            // not available in the specified time period. This status code doesn't indicate the result
            // of operation, it might be completed or cancelled.
            OperationTimeout = TimeSpan.FromSeconds(1),

            // Transport timeout from the moment operation was sent to server. It is useful in case
            // of possible network issues, to that query doesn't hang forever.
            // It is recommended to set this value to a larger value than OperationTimeout to give
            // server some time to issue a response.
            TransportTimeout = TimeSpan.FromSeconds(5),

            // Keep query compilation result in query cache or not. Should be false for ad-hoc queries,
            // and true (default) for high-RPS queries.
            KeepInQueryCache = false
        };

    private ExecuteDataQuerySettings DefaultCachedDataQuerySettings
    {
        get
        {
            var settings = DefaultDataQuerySettings;
            settings.KeepInQueryCache = true;
            return settings;
        }
    }
}