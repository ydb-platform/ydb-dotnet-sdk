using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterQueryYdbFixture : SharedStoreFixtureBase<DecimalParameterQueryYdbFixture.TestContext>
{
    protected override string StoreName => "DecimalParameterTest";

    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    public class TestContext : DbContext
    {
        public TestContext(DbContextOptions options) : base(options)
        {
        }

        public DbSet<ItemDefault> ItemsDefault => Set<ItemDefault>();
        public DbSet<ItemExplicit> ItemsExplicit => Set<ItemExplicit>();

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
