// using Microsoft.EntityFrameworkCore;

using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Section_1.HR;

public class HRContext : DbContext
{
    public DbSet<Employee> Employees { get; set; }
    public DbSet<Department> Departments { get; set; }

    public HRContext()
    {
    }

    public HRContext(DbContextOptions<HRContext> options) : base(options)
    {
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
        optionsBuilder
            .UseYdb("Host=localhost;Port=2136;Database=/local")
            .LogTo(Console.WriteLine, new[] { DbLoggerCategory.Database.Command.Name }, LogLevel.Information);
}

internal class HRContextContextFactory : IDesignTimeDbContextFactory<HRContext>
{
    public HRContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<HRContext>();

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
                .UseYdb("Host=localhost;Port=2136;Database=/local", builder => builder.DisableRetryOnFailure())
                .ConfigureWarnings(warnings => warnings.Ignore(RelationalEventId.PendingModelChangesWarning))
                .Options
        );
    }
}