using CommandLine;

namespace AdoNet;

internal class CmdOptions
{
    [Option('h', "host", Required = false, HelpText = "Database host")]
    public string Host { get; set; } = "localhost";

    [Option('h', "host", Required = false, HelpText = "Database port")]
    public int Port { get; set; } = 2136;

    [Option('d', "database", Required = false, HelpText = "Database name")]
    public string Database { get; set; } = "/local";

    [Option("useTls", Required = false, HelpText = "Using tls")]
    public bool UseTls { get; set; } = false;

    [Option("useTls", Required = false, HelpText = "Tls port")]
    public int TlsPort { get; set; } = 2135;

    public string SimpleConnectionString => $"Host={Host};Port={Port};Database={Database}";
}