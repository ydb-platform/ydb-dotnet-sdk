using System.Diagnostics;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Yc;

var connectionString = new ConfigurationBuilder()
                           .SetBasePath(Directory.GetCurrentDirectory())
                           .AddJsonFile("appsettings.json")
                           .Build()
                           .GetConnectionString("ServerlessYDB") ??
                       throw new InvalidOperationException("ConnectionString.ServerlessYDB is empty.");

var loggerFactory = LoggerFactory.Create(builder => builder.AddNLog());
var logger = loggerFactory.CreateLogger<Program>();

var stopwatch = Stopwatch.StartNew();
stopwatch.Start();

await using var ydbDataSource = new YdbDataSource(new YdbConnectionStringBuilder(connectionString)
{
    CredentialsProvider = new MetadataProvider(loggerFactory: loggerFactory),
    LoggerFactory = loggerFactory,
    DisableDiscovery = true,
    EnableImplicitSession = true
});

await using var ydbConnection = await ydbDataSource.OpenRetryableConnectionAsync();
var scalar = await ydbConnection.ExecuteScalarAsync<string>(
    "SELECT 'Hello Serverless YDB from Yandex Cloud Serverless Container!'u");
stopwatch.Stop();

logger.LogInformation("Success request! [Ms: {Ms}], {Select}", stopwatch.ElapsedMilliseconds, scalar);