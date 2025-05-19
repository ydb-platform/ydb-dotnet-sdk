using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class FieldsOnlyLoadYdbTest(FieldsOnlyLoadYdbTest.FieldsOnlyLoadYdbFixture fixture)
    : FieldsOnlyLoadTestBase<FieldsOnlyLoadYdbTest.FieldsOnlyLoadYdbFixture>(fixture)
{
    public class FieldsOnlyLoadYdbFixture : FieldsOnlyLoadFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
