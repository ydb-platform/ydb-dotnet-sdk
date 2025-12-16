using CommandLine;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.AdoNet.Yandex.Cloud;
using Ydb.Sdk.Yc;

await Parser.Default.ParseArguments<CmdOptions>(args).WithParsedAsync(async cmd =>
{
    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
    var saProvider = new ServiceAccountProvider(saFilePath: cmd.SaFilePath, loggerFactory: loggerFactory);

    await using var ydbDataSource = new YdbDataSource(new YdbConnectionStringBuilder(cmd.ConnectionString)
    {
        CredentialsProvider = saProvider,
        LoggerFactory = loggerFactory,
        ServerCertificates = YcCerts.GetYcServerCertificates()
    });
    await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();

    Console.WriteLine(await new YdbCommand(ydbConnection) { CommandText = "SELECT 'Hello YDB from Yandex Cloud!'u" }
        .ExecuteScalarAsync());
});