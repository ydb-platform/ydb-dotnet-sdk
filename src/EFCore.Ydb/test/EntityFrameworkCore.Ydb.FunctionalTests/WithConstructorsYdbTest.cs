using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class WithConstructorsYdbTest(WithConstructorsYdbTest.WithConstructorsYdbFixture fixture)
    : WithConstructorsTestBase<WithConstructorsYdbTest.WithConstructorsYdbFixture>(fixture)
{
    [ConditionalFact(Skip = "Cannot create table without key")]
    public override void Query_with_keyless_type() => base.Query_with_keyless_type();

    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    public class WithConstructorsYdbFixture : WithConstructorsFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);

            // Necessary because YDB cannot create tables without keys
            modelBuilder.Entity<BlogQuery>().HasKey(x => x.Title);
        }
    }
}
