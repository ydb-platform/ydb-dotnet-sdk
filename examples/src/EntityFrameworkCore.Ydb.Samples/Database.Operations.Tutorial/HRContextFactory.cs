using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace Section_3.ProjectEF;

internal class HRContextFactory : IDesignTimeDbContextFactory<HRContext>
{
    public HRContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<HRContext>();

        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json")
            .Build();

        var connectionString = configuration.GetConnectionString("Local");
        
        // IMPORTANT!
        // Disables retries for the migrations context.
        // Required because migration operations may use suppressed or explicit transactions,
        // and enabling retries in this case leads to runtime errors with this provider.
        //
        // "System.NotSupportedException: User transaction is not supported with a TransactionSuppressed migrations or a retrying execution strategy."
        //
        // Bottom line: ALWAYS disable retries for design-time/migration contexts to avoid migration failures and errors.
        return new HRContext(
            optionsBuilder
                .UseYdb(connectionString, builder => builder.DisableRetryOnFailure())
                .ConfigureWarnings(warnings => warnings.Ignore(RelationalEventId.PendingModelChangesWarning))
                .Options
        );
    }
}