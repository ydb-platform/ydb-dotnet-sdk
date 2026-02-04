using CommandLine;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.AdoNet.Yandex.Cloud;
using Ydb.Sdk.Yc;

await Parser.Default.ParseArguments<CmdOptions>(args).WithParsedAsync(async cmd =>
{
    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
    var saProvider = new ServiceAccountProvider(saFilePath: cmd.SaFilePath, loggerFactory: loggerFactory);

    // First example with YdbConnectionStringBuilder
    await using var ydbDataSource = new YdbDataSource(new YdbConnectionStringBuilder(cmd.ConnectionString)
    {
        CredentialsProvider = saProvider,
        LoggerFactory = loggerFactory,
        ServerCertificates = YcCerts.GetYcServerCertificates()
    });
    await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();

    Console.WriteLine(await new YdbCommand(ydbConnection)
            { CommandText = "SELECT 'Hello YDB from Yandex Cloud with YdbConnectionStringBuilder!'u" }
        .ExecuteScalarAsync());

    // Second example with connectionString
    await using var ydbDataSourceAnother = new YdbDataSource(
        new YdbConnectionStringBuilder($"{cmd.ConnectionString};ServiceAccountKeyFilePath={cmd.SaFilePath};")
            { LoggerFactory = loggerFactory }
    );
    await using var ydbConnectionAnother = await ydbDataSourceAnother.OpenConnectionAsync();

    Console.WriteLine(await new YdbCommand(ydbConnectionAnother)
            { CommandText = "SELECT 'Hello YDB from Yandex Cloud with simple ConnectionString!'u" }
        .ExecuteScalarAsync());
});