using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class FieldMappingYdbTest(FieldMappingYdbTest.FieldMappingYdbFixture fixture)
    : FieldMappingTestBase<FieldMappingYdbTest.FieldMappingYdbFixture>(fixture)
{
    protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
        => facade.UseTransaction(transaction.GetDbTransaction());

    public class FieldMappingYdbFixture : FieldMappingFixtureBase
    {
        protected override string StoreName => "FieldMapping";

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }

    [ConditionalFact(Skip = "OrderBy parameter not included in SELECT")]
    public override void Can_define_a_backing_field_for_a_navigation_and_query_and_update_it() =>
        base.Can_define_a_backing_field_for_a_navigation_and_query_and_update_it();
}
