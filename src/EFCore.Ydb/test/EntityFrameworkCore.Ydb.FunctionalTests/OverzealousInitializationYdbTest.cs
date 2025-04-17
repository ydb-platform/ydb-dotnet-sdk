using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class OverzealousInitializationYdbTest(
    OverzealousInitializationYdbTest.OverzealousInitializationYdbFixture fixture
)
    : OverzealousInitializationTestBase<OverzealousInitializationYdbTest.OverzealousInitializationYdbFixture>(fixture)
{
    public class OverzealousInitializationYdbFixture : OverzealousInitializationFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
