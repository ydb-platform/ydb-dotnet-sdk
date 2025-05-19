using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

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
