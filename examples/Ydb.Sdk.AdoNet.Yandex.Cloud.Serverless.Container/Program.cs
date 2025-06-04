// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Yc;

var connectionString = new ConfigurationBuilder()
                           .SetBasePath(Directory.GetCurrentDirectory())
                           .AddJsonFile("appsettings.json")
                           .Build()
                           .GetConnectionString("ServerlessYDB") ??
                       throw new InvalidOperationException("ConnectionString.ServerlessYDB is empty.");

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
var logger = loggerFactory.CreateLogger<Program>();
var stopwatch = Stopwatch.StartNew();
stopwatch.Start();

await using var dataSource = new YdbDataSource(
    new YdbConnectionStringBuilder(connectionString)
    {
        CredentialsProvider = new MetadataProvider(loggerFactory: loggerFactory),
        LoggerFactory = loggerFactory,
        DisableDiscovery = true
    }
);

await using var ydbCommand = dataSource.CreateCommand();
ydbCommand.CommandText = "SELECT 'Hello Serverless YDB from Yandex Cloud Serverless Container!'u";
var scalar = await ydbCommand.ExecuteScalarAsync();
stopwatch.Stop();

logger.LogInformation("Success request! [Ms: {Ms}], {Select}", stopwatch.ElapsedMilliseconds, scalar);