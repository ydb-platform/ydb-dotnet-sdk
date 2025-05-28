using CommandLine;

namespace YcCloud;

internal class CmdOptions
{
    [Option("connectionString", Required = true, HelpText = "ConnectionString ADO.NET format")]
    public string ConnectionString { get; set; } = null!;

    [Option("saFilePath", Required = true, HelpText = "Sa Key")]
    public string SaFilePath { get; set; } = null!;
}