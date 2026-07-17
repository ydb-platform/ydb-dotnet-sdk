using System.Reflection;
using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class QuerySessionSdkBuildInfoTests
{
    // Reproduces EfCoreYdbVersion.Value (internal) from the same assembly the provider reports from.
    private static readonly string EfCoreClientInfo =
        "ef-core/" + (typeof(YdbContextOptionsBuilderExtensions).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "UNKNOWN");

    [Fact]
    public async Task QuerySession_ReportsSdkBuildInfoWithEfCoreClientInfo()
    {
        var pid = Environment.ProcessId.ToString();

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
            info.EndsWith(";" + EfCoreClientInfo));
    }

    private sealed class SdkBuildInfoDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136", builder => builder.DisableRetryOnFailure())
            .EnableServiceProviderCaching(false);
    }
}
