using System.CommandLine;

namespace Internal;

public static class Cli
{
    private static readonly Argument<string> ConnectionStringArgument = new(
        "connectionString",
        "YDB connection string ADO NET format");

    private static readonly Option<string?> OtlpEndpointOption = new(
        "--otlp-endpoint",
        "OpenTelemetry OTLP metrics endpoint URL. Falls back to OTEL_EXPORTER_OTLP_METRICS_ENDPOINT / OTEL_EXPORTER_OTLP_ENDPOINT env vars.");

    private static readonly Option<int> WriteTimeoutOption = new(
        "--write-timeout",
        () => 100,
        "write timeout seconds");

    private static readonly Option<int> ReportPeriodOption = new(
        "--report-period",
        () => 1000,
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
        () => int.TryParse(Environment.GetEnvironmentVariable("WORKLOAD_DURATION"), out var t) ? t : 600,
        "run time in seconds. Falls back to WORKLOAD_DURATION env var.");

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
        "runs workload (read and write to table with sets RPS). Creates table and seeds initial data if missing.")
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
                TimeOption,
                InitialDataCountOption
            )
        );

        return await RootCommand.InvokeAsync(args);
    }
}