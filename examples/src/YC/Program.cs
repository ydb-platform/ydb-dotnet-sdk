using CommandLine;
using Microsoft.Extensions.Logging;
using YcCloud;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Yc;

await Parser.Default.ParseArguments<CmdOptions>(args).WithParsedAsync(async cmd =>
{
    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

    var saProvider = new ServiceAccountProvider(saFilePath: cmd.SaFilePath, loggerFactory: loggerFactory);
    await saProvider.Initialize();

    var cert = YcCerts.GetDefaultServerCertificate();

    var builder = new YdbConnectionStringBuilder
    {
        UseTls = true,
        Host = cmd.Host,
        Port = 2135,
        Database = cmd.Database,
        CredentialsProvider = saProvider,
        LoggerFactory = loggerFactory,
        CustomCertificate = cert
    };

    await using var ydbConnection = new YdbConnection(builder);
    await ydbConnection.OpenAsync();

    Console.WriteLine(await new YdbCommand(ydbConnection) { CommandText = "SELECT 'Hello YDB!'u" }.ExecuteScalarAsync());
});