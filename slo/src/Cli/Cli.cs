using System.CommandLine;

namespace slo.Cli;

internal static class Cli
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

    private static readonly Option<FileInfo?> X509CertPathOption = new(
        "--cert",
        result =>
        {
            const string defaultX590Path = "/ydb-ca.pem";
            if (result.Tokens.Count == 0)
            {
                return new FileInfo(defaultX590Path);
            }

            var filePath = result.Tokens.Single().Value;
            if (File.Exists(filePath))
            {
                return new FileInfo(filePath);
            }

            result.ErrorMessage = "File does not exist";
            return null;
        },
        true,
        "Path to x509 certificate file"
    );

    private static readonly Command CreateCommand = new(
        "create",
        "creates table in database")
    {
        TableOption,
        WriteTimeoutOption,
        EndpointArgument,
        DbArgument,
        MinPartitionsCountOption,
        MaxPartitionsCountOption,
        PartitionSizeOption,
        InitialDataCountOption,
        X509CertPathOption
    };


    private static readonly Command CleanupCommand = new(
        "cleanup",
        "drops table in database")
    {
        TableOption,
        WriteTimeoutOption,
        EndpointArgument,
        DbArgument,
        X509CertPathOption
    };

    private static readonly Command RunCommand = new(
        "run",
        "runs workload (read and write to table with sets RPS)")
    {
        TableOption,
        WriteTimeoutOption,
        EndpointArgument,
        DbArgument,
        PromPgwOption,
        ReportPeriodOption,
        ReadRpsOption,
        ReadTimeoutOption,
        WriteRpsOption,
        TimeOption,
        ShutdownTimeOption,
        X509CertPathOption
    };

    private static readonly RootCommand RootCommand = new("SLO app")
    {
        CreateCommand, CleanupCommand, RunCommand
    };

    internal static async Task<int> Run(string[] args)
    {
        CreateCommand.SetHandler(
            async createConfig => { await CliCommands.Create(createConfig); },
            new CreateConfigBinder(EndpointArgument, DbArgument, TableOption, MinPartitionsCountOption,
                MaxPartitionsCountOption, PartitionSizeOption, InitialDataCountOption, WriteTimeoutOption,
                X509CertPathOption)
        );

        CleanupCommand.SetHandler(
            async cleanUpConfig => { await CliCommands.CleanUp(cleanUpConfig); },
            new CleanUpConfigBinder(EndpointArgument, DbArgument, TableOption, WriteTimeoutOption, X509CertPathOption)
        );

        RunCommand.SetHandler(async runConfig => { await CliCommands.Run(runConfig); },
            new RunConfigBinder(EndpointArgument, DbArgument, TableOption, InitialDataCountOption, PromPgwOption,
                ReportPeriodOption, ReadRpsOption, ReadTimeoutOption, WriteRpsOption, WriteTimeoutOption, TimeOption,
                ShutdownTimeOption, X509CertPathOption));
        return await RootCommand.InvokeAsync(args);
    }
}