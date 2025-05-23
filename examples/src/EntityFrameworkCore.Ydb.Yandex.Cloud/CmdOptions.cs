using CommandLine;

namespace EntityFrameworkCore.Ydb.Yandex.Cloud;

internal class CmdOptions
{
    [Option("connectionString", Required = true, HelpText = "Connection string")]
    public string ConnectionString { get; set; } = null!;

    [Option("saFilePath", Required = true, HelpText = "Sa Key")]
    public string SaFilePath { get; set; } = null!;
}