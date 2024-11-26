using System.CommandLine;

namespace Internal;

public static class Cli
{
    private static readonly Argument<string> EndpointArgument = new(
        "endpoint",
        "YDB endpoint to connect to");

    private static readonly Argument<string> DbArgument = new(
        "db",
        "YDB database to connect to");

    private static readonly Option<string> PromPgwOption = new(
        "--prom-pgw",
        "minimum amount of partitions in table") { IsRequired = true };

    private static readonly Option<string> ResourceYdbPath = new(
        new[] { "-t", "--table-name" },
        () => "testingTable",
        "table name to create\n ");

    private static readonly Option<int> WriteTimeoutOption = new(
        "--write-timeout",
        () => 1000,
        "write timeout seconds");

    private static readonly Option<int> ReportPeriodOption = new(
        "--report-period",
        () => 250,
        "prometheus push period in milliseconds");

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

    private static readonly Option<int> MinPartitionsCountOption = new(
        "--min-partitions-count",
        () => 5,
        "minimum amount of partitions in table");

    private static readonly Option<int> MaxPartitionsCountOption = new(
        "--max-partitions-count",
        () => 10,
        "maximum amount of partitions in table");

    private static readonly Option<int> InitialDataCountOption = new(
        new[] { "-c", "--initial-data-count" },
        () => 1000,
        "amount of initially created rows");

    private static readonly Command CreateCommand = new(
        "create",
        "creates table in database")
    {
        EndpointArgument,
        DbArgument,
        ResourceYdbPath,
        MinPartitionsCountOption,
        MaxPartitionsCountOption,
        InitialDataCountOption,
        WriteTimeoutOption
    };

    private static readonly Command RunCommand = new(
        "run",
        "runs workload (read and write to table with sets RPS)")
    {
        EndpointArgument,
        DbArgument,
        InitialDataCountOption,
        PromPgwOption,
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

    public static async Task<int> Run<T>(SloContext<T> sloContext, string[] args) where T : IDisposable
    {
        CreateCommand.SetHandler(async createConfig => { await sloContext.Create(createConfig); },
            new CreateConfigBinder(EndpointArgument, DbArgument, ResourceYdbPath, MinPartitionsCountOption,
                MaxPartitionsCountOption, InitialDataCountOption, WriteTimeoutOption));

        RunCommand.SetHandler(async runConfig => { await sloContext.Run(runConfig); },
            new RunConfigBinder(EndpointArgument, DbArgument, ResourceYdbPath, PromPgwOption, ReportPeriodOption,
                ReadRpsOption, ReadTimeoutOption, WriteRpsOption, WriteTimeoutOption, TimeOption));

        return await RootCommand.InvokeAsync(args);
    }
}