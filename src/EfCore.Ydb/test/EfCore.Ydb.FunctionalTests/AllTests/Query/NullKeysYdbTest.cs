using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.AllTests.Query;

public class NullKeysYdbTest(NullKeysYdbTest.NullKeysYdbFixture fixture)
    : NullKeysTestBase<NullKeysYdbTest.NullKeysYdbFixture>(fixture)
{
    public class NullKeysYdbFixture : NullKeysFixtureBase
    {
        protected override string StoreName => "StringsContext";

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
