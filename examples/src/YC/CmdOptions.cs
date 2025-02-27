using CommandLine;

namespace YcCloud;

internal class CmdOptions
{
    [Option('h', "host", Required = true, HelpText = "Database host")]
    public string Host { get; set; } = null!;

    [Option('d', "database", Required = true, HelpText = "Database name")]
    public string Database { get; set; } = null!;
    
    [Option("saFilePath", Required = true, HelpText = "Sa Key")]
    public string SaFilePath { get; set; } = null!;
}