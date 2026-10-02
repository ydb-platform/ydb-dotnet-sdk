using System.Text.Json;

namespace Internal;

public sealed record SloCheck(string Id, string Verdict, string Detail);

public sealed record SloOperationCount(string Type, long Success, long Error);

public static class SloResult
{
    public static string Reference => Environment.GetEnvironmentVariable("WORKLOAD_REF") ?? "current";

    public static async Task WriteAsync(
        string runId,
        string kind,
        IReadOnlyList<SloCheck> checks,
        IReadOnlyList<SloOperationCount> operations)
    {
        var verdict = checks.Any(check => check.Verdict == "FAIL")
            ? "FAIL"
            : checks.Any(check => check.Verdict != "PASS")
                ? "INVALID"
                : "PASS";
        var result = new
        {
            SchemaVersion = 3,
            Ref = Reference,
            RunId = runId,
            Kind = kind,
            Verdict = verdict,
            Checks = checks,
            Operations = operations
        };
        var content = JsonSerializer.Serialize(result, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var path = Environment.GetEnvironmentVariable("SLO_RESULT_PATH") ?? "/tmp/slo-result.json";
        await File.WriteAllTextAsync(path, content);
        Console.WriteLine(content);
    }
}