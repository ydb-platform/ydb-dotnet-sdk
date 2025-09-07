using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterQueryYdbFixture
    : SharedStoreFixtureBase<DecimalParameterQueryYdbFixture.DecimalContext>
{
    private static DbCommandInterceptor? CurrentInterceptor => null;

    protected override string StoreName => "DecimalParameter";
    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
    {
        var b = base.AddOptions(builder);
        b.UseYdb(GetConnString());
        if (CurrentInterceptor is not null)
            b.AddInterceptors(CurrentInterceptor);
        return b;
    }

    protected override void OnModelCreating(ModelBuilder b, DbContext ctx)
    {
        b.Entity<ItemDefault>(e => e.HasKey(x => x.Id));

        b.Entity<ItemExplicit>(e =>
        {
            e.HasKey(x => x.Id);
            e.Property(x => x.Price).HasPrecision(22, 9);
        });
    }

    private static string GetConnString()
    {
        var cs = Environment.GetEnvironmentVariable("YDB_EF_CONN");
        if (!string.IsNullOrWhiteSpace(cs)) return cs;

        var endpoint = Environment.GetEnvironmentVariable("YDB_ENDPOINT") ?? "grpc://localhost:2136";
        var database = Environment.GetEnvironmentVariable("YDB_DATABASE") ?? "/ef-tests";
        return $"{endpoint};database={database}";
    }

    public class DecimalContext(DbContextOptions options) : DbContext(options);
}

public class ItemDefault
{
    public int Id { get; init; }
    public decimal Price { get; init; }
}

public class ItemExplicit
{
    public int Id { get; init; }
    public decimal Price { get; init; }
}
