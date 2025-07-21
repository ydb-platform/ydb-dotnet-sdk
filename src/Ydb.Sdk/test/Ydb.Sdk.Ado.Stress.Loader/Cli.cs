using System.CommandLine;
using System.CommandLine.Binding;

namespace Ydb.Sdk.Ado.Stress.Loader;

public static class Cli
{
    private static readonly Argument<string> ConnectionString = new(
        "connectionString",
        "YDB connection string ADO NET format"
    );

    private static readonly Option<int> PeakRps = new("--peak-rps", () => 1000,
        "Peak RPS load (top of the step)");

    private static readonly Option<int> MediumRps = new("--medium-rps", () => 100,
        "Medium RPS load (middle of the step)");

    private static readonly Option<int> MinRps = new("--min-rps", () => 1,
        "Minimum RPS load (bottom of the step, 1-2 RPS)");

    private static readonly Option<int> PeakDurationSeconds = new("--peak-duration", () => 600,
        "Duration of peak load in seconds");

    private static readonly Option<int> MediumDurationSeconds = new("--medium-duration", () => 1800,
        "Duration of medium load in seconds");

    private static readonly Option<int> MinDurationSeconds = new("--min-duration", () => 1800,
        "Duration of minimum load in seconds");

    private static readonly Option<int> TotalTestTimeSeconds = new("--total-time", () => 14400,
        "Total test duration in seconds");

    private static readonly Option<string?> SaFilePath = new("--sa-file-path",
        "Path to Service Account file for authentication");

    private static readonly Option<string> TestQuery = new("--test-query",
        () => "SELECT 1 as test_column",
        "SQL query to execute during stress test"
    );

    public static readonly RootCommand RootCommand = new("YDB ADO.NET Stress Test Tank - Variable Load Generator")
    {
        ConnectionString,
        PeakRps,
        MediumRps,
        MinRps,
        PeakDurationSeconds,
        MediumDurationSeconds,
        MinDurationSeconds,
        TotalTestTimeSeconds,
        SaFilePath,
        TestQuery
    };

    static Cli()
    {
        RootCommand.SetHandler(async config =>
            {
                var stressLoader = new StressTestTank(config);
                await stressLoader.RunAsync();
            },
            new ConfigBinder(
                ConnectionString,
                PeakRps,
                MediumRps,
                MinRps,
                PeakDurationSeconds,
                MediumDurationSeconds,
                MinDurationSeconds,
                TotalTestTimeSeconds,
                SaFilePath,
                TestQuery
            )
        );
    }
}

public class ConfigBinder(
    Argument<string> connectionString,
    Option<int> peakRps,
    Option<int> mediumRps,
    Option<int> minRps,
    Option<int> peakDurationSeconds,
    Option<int> mediumDurationSeconds,
    Option<int> minDurationSeconds,
    Option<int> totalTestTimeSeconds,
    Option<string?> saFilePath,
    Option<string> testQuery
) : BinderBase<StressTestConfig>
{
    protected override StressTestConfig GetBoundValue(BindingContext bindingContext) => new(
        ConnectionString: bindingContext.ParseResult.GetValueForArgument(connectionString),
        PeakRps: bindingContext.ParseResult.GetValueForOption(peakRps),
        MediumRps: bindingContext.ParseResult.GetValueForOption(mediumRps),
        MinRps: bindingContext.ParseResult.GetValueForOption(minRps),
        PeakDurationSeconds: bindingContext.ParseResult.GetValueForOption(peakDurationSeconds),
        MediumDurationSeconds: bindingContext.ParseResult.GetValueForOption(mediumDurationSeconds),
        MinDurationSeconds: bindingContext.ParseResult.GetValueForOption(minDurationSeconds),
        TotalTestTimeSeconds: bindingContext.ParseResult.GetValueForOption(totalTestTimeSeconds),
        SaFilePath: bindingContext.ParseResult.GetValueForOption(saFilePath),
        TestQuery: bindingContext.ParseResult.GetValueForOption(testQuery)!
    );
}

public record StressTestConfig(
    string ConnectionString,
    int PeakRps,
    int MediumRps,
    int MinRps,
    int PeakDurationSeconds,
    int MediumDurationSeconds,
    int MinDurationSeconds,
    int TotalTestTimeSeconds,
    string? SaFilePath,
    string TestQuery
);
