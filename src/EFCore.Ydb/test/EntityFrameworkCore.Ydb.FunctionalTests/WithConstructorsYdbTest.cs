using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class WithConstructorsYdbTest(WithConstructorsYdbTest.WithConstructorsYdbFixture fixture)
    : WithConstructorsTestBase<WithConstructorsYdbTest.WithConstructorsYdbFixture>(fixture)
{
    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    // TODO: Keyless :c
    public override void Query_with_keyless_type()
    {
    }

    public class WithConstructorsYdbFixture : WithConstructorsFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
           base.OnModelCreating(modelBuilder, context);
           modelBuilder.Entity<BlogQuery>().HasKey(x => x.Title);
        }
    }
}
