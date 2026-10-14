using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class CompositeKeyEndToEndYdbTest(CompositeKeyEndToEndYdbTest.CompositeKeyEndToEndYdbFixture fixture)
    : CompositeKeyEndToEndTestBase<CompositeKeyEndToEndYdbTest.CompositeKeyEndToEndYdbFixture>(fixture)
{
    public class CompositeKeyEndToEndYdbFixture : CompositeKeyEndToEndFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
