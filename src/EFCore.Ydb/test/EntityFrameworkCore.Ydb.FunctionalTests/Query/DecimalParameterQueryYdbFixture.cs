using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterQueryYdbFixture : SharedStoreFixtureBase<DecimalParameterQueryYdbFixture.TestContext>
{
    protected override string StoreName => "DecimalParameterTest";

    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    public class TestContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ItemDefault>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.Price);
            });

            modelBuilder.Entity<ItemExplicit>(b =>
            {
                b.HasKey(x => x.Id);
                b.Property(x => x.Price).HasPrecision(22, 9);
            });
        }
    }
}

public class ItemDefault
{
    public int Id { get; set; }
    public decimal Price { get; set; }
}

public class ItemExplicit
{
    public int Id { get; set; }
    public decimal Price { get; set; }
}
