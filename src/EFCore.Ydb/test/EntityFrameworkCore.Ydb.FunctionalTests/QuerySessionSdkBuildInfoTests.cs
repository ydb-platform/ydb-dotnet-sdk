using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class QuerySessionSdkBuildInfoTests
{
    [Fact]
    public async Task QuerySession_ReportsSdkBuildInfoWithEfCoreClientInfo()
    {
        var pid = Environment.ProcessId.ToString();
        var efCoreClientInfo = $"ef-core/{EfCoreYdbVersion.Value}";

        await using var dbContext = new SdkBuildInfoDbContext();

        // Shared YDB in CI may have many sessions for this PID; assert at least one carries the EF chain.
        var buildInfos = await dbContext.Database
            .SqlQuery<string>(
                $"SELECT ClientSdkBuildInfo AS Value FROM `.sys/query_sessions` WHERE ClientPID = {pid}")
            .ToListAsync();

        Assert.Contains(buildInfos, info =>
            info != null &&
            info.Contains("ydb-dotnet-sdk/", StringComparison.Ordinal) &&
            info.Contains(";ado-net/", StringComparison.Ordinal) &&
            info.Contains(efCoreClientInfo, StringComparison.Ordinal));
    }

    private sealed class SdkBuildInfoDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136", builder => builder.DisableRetryOnFailure())
            .EnableServiceProviderCaching(false);
    }
}
