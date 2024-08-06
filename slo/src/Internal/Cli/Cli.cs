using System.CommandLine;

namespace Internal.Cli;

public static class Cli
{
    private static readonly Argument<string> EndpointArgument = new(
        "endpoint",
        "YDB endpoint to connect to");

    private static readonly Argument<string> DbArgument = new(
        "db",
        "YDB database to connect to");

    private static readonly Option<string> TableOption = new(
        new[] { "-t", "--table-name" },
        () => "testingTable",
        "table name to create\n ");

    private static readonly Option<int> WriteTimeoutOption = new(
        "--write-timeout",
        () => 10000,
        "write timeout milliseconds");

    private static readonly Option<int> MinPartitionsCountOption = new(
        "--min-partitions-count",
        () => 6,
        "minimum amount of partitions in table");

    private static readonly Option<int> MaxPartitionsCountOption = new(
        "--max-partitions-count",
        () => 1000,
        "maximum amount of partitions in table");

    private static readonly Option<int> PartitionSizeOption = new(
        "--partition-size",
        () => 1,
        "partition size in mb");

    private static readonly Option<int> InitialDataCountOption = new(
        new[] { "-c", "--initial-data-count" },
        () => 1000,
        "amount of initially created rows");

    private static readonly Option<string> PromPgwOption = new(
        "--prom-pgw",
        "minimum amount of partitions in table") { IsRequired = true };

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
        () => 10000,
        "read timeout milliseconds");

    private static readonly Option<int> WriteRpsOption = new(
        "--write-rps",
        () => 100,
        "write RPS");

    private static readonly Option<int> TimeOption = new(
        "--time",
        () => 140,
        "run time in seconds");

    private static readonly Option<int> ShutdownTimeOption = new(
        "--shutdown-time",
        () => 30,
        "time to wait before force kill workers");

    private static readonly Command CreateCommand = new(
        "create",
        "creates table in database")
    {
        EndpointArgument,
        DbArgument,
        TableOption,
        MinPartitionsCountOption,
        MaxPartitionsCountOption,
        PartitionSizeOption,
        InitialDataCountOption,
        WriteTimeoutOption
    };

    private static readonly Command CleanupCommand = new(
        "cleanup",
        "drops table in database")
    {
        EndpointArgument,
        DbArgument,
        TableOption,
        WriteTimeoutOption
    };

    private static readonly Command RunCommand = new(
        "run",
        "runs workload (read and write to table with sets RPS)")
    {
        EndpointArgument,
        DbArgument,
        TableOption,
        InitialDataCountOption,
        PromPgwOption,
        ReportPeriodOption,
        ReadRpsOption,
        ReadTimeoutOption,
        WriteRpsOption,
        WriteTimeoutOption,
        TimeOption,
        ShutdownTimeOption
    };

    private static readonly RootCommand RootCommand = new("SLO app")
    {
        CreateCommand, CleanupCommand, RunCommand
    };

    public static async Task<int> Run(string[] args)
    {
        CreateCommand.SetHandler(async createConfig => { await CliCommands.Create(createConfig); },
            new CreateConfigBinder(EndpointArgument, DbArgument, TableOption, MinPartitionsCountOption,
                MaxPartitionsCountOption, PartitionSizeOption, InitialDataCountOption, WriteTimeoutOption));

        CleanupCommand.SetHandler(async cleanUpConfig => { await CliCommands.CleanUp(cleanUpConfig); },
            new CleanUpConfigBinder(EndpointArgument, DbArgument, TableOption, WriteTimeoutOption));

        RunCommand.SetHandler(async runConfig => { await CliCommands.Run(runConfig); },
            new RunConfigBinder(EndpointArgument, DbArgument, TableOption, PromPgwOption, ReportPeriodOption,
                ReadRpsOption, ReadTimeoutOption, WriteRpsOption, WriteTimeoutOption, TimeOption, ShutdownTimeOption));

        return await RootCommand.InvokeAsync(args);
    }
}