using CommandLine;

namespace EF_YC;

internal class CmdOptions
{
    [Option('c', "connectionString", Required = true, HelpText = "Connection string")]
    public string ConnectionString { get; set; } = null!;

    [Option("saFilePath", Required = true, HelpText = "Sa Key")]
    public string SaFilePath { get; set; } = null!;
}