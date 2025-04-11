using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.AllTests.Query;

public abstract class FieldsOnlyLoadYdbTest(FieldsOnlyLoadYdbTest.FieldsOnlyLoadYdbFixture fixture)
    : FieldsOnlyLoadTestBase<FieldsOnlyLoadYdbTest.FieldsOnlyLoadYdbFixture>(fixture)
{
    public abstract class FieldsOnlyLoadYdbFixture : FieldsOnlyLoadFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
