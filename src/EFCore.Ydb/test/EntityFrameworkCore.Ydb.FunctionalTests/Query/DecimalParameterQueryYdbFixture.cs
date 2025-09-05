using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
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
