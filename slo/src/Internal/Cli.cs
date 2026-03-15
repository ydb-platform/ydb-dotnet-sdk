using System.CommandLine;

namespace Internal;

public static class Cli
{
    private static readonly Argument<string> ConnectionStringArgument = new(
        "connectionString",
        "YDB connection string ADO NET format");

    private static readonly Option<string> OtlpEndpointOption = new(
        "--otlp-endpoint",
        "OpenTelemetry OTLP endpoint URL (e.g., http://localhost:4318)");

    private static readonly Option<int> WriteTimeoutOption = new(
        "--write-timeout",
        () => 100,
        "write timeout seconds");

    private static readonly Option<int> ReportPeriodOption = new(
        "--report-period",
        () => 250,
        "metrics export period in milliseconds");

    private static readonly Option<int> ReadRpsOption = new(
        "--read-rps",
        () => 1000,
        "read RPS");

    private static readonly Option<int> ReadTimeoutOption = new(
        "--read-timeout",
        () => 1000,
        "read timeout seconds");

    private static readonly Option<int> WriteRpsOption = new(
        "--write-rps",
        () => 1000,
        "write RPS");

    private static readonly Option<int> TimeOption = new(
        "--time",
        () => 600,
        "run time in seconds");

    private static readonly Option<int> InitialDataCountOption = new(
        new[] { "-c", "--initial-data-count" },
        () => 1000,
        "amount of initially created rows");

    private static readonly Command CreateCommand = new(
        "create",
        "creates table in database")
    {
        ConnectionStringArgument,
        InitialDataCountOption,
        WriteTimeoutOption
    };

    private static readonly Command RunCommand = new(
        "run",
        "runs workload (read and write to table with sets RPS)")
    {
        ConnectionStringArgument,
        InitialDataCountOption,
        OtlpEndpointOption,
        ReportPeriodOption,
        ReadRpsOption,
        ReadTimeoutOption,
        WriteRpsOption,
        WriteTimeoutOption,
        TimeOption
    };

    private static readonly RootCommand RootCommand = new("SLO app")
    {
        CreateCommand, RunCommand
    };

    public static async Task<int> Run(ISloContext sloContext, string[] args)
    {
        CreateCommand.SetHandler(
            async createConfig => { await sloContext.Create(createConfig); },
            new CreateConfigBinder(
                ConnectionStringArgument,
                InitialDataCountOption,
                WriteTimeoutOption
            )
        );

        RunCommand.SetHandler(
            async runConfig => { await sloContext.Run(runConfig); },
            new RunConfigBinder(
                ConnectionStringArgument,
                OtlpEndpointOption,
                ReportPeriodOption,
                ReadRpsOption,
                ReadTimeoutOption,
                WriteRpsOption,
                WriteTimeoutOption,
                TimeOption
            )
        );

        return await RootCommand.InvokeAsync(args);
    }
}