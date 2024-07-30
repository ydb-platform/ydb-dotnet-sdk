using CommandLine;

namespace AdoNet;

internal class CmdOptions
{
    [Option('h', "host", Required = true, HelpText = "Database host")]
    public string Host { get; set; } = "localhost";

    [Option('h', "host", Required = true, HelpText = "Database port")]
    public int Port { get; set; } = 2136;

    [Option('d', "database", Required = true, HelpText = "Database name")]
    public string Database { get; set; } = "/local";

    public string ConnectionString => $"Host={Host};Port={Port};Database={Database}";
}