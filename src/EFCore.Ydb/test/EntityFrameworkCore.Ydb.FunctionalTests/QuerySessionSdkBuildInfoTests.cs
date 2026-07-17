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

        // The session running this query was created by the EF Core provider (layered on ADO.NET), so its
        // ClientSdkBuildInfo must carry the base SDK token, the ado-net component and the ef-core component.
        var buildInfos = await dbContext.Database
            .SqlQuery<string>(
                $"SELECT ClientSdkBuildInfo AS Value FROM `.sys/query_sessions` WHERE ClientPID = {pid}")
            .ToListAsync();

        Assert.Contains(buildInfos, info =>
            info != null &&
            info.StartsWith("ydb-dotnet-sdk/") &&
            info.Contains(";ado-net/") &&
            info.EndsWith(";" + efCoreClientInfo));
    }

    private sealed class SdkBuildInfoDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136", builder => builder.DisableRetryOnFailure())
            .EnableServiceProviderCaching(false);
    }
}
