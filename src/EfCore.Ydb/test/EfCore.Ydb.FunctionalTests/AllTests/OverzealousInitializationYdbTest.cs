using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.AllTests;

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
