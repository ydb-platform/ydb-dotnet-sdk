using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public abstract class FindYdbTest : FindTestBase<FindYdbTest.FindYdbFixture>
{
    protected FindYdbTest(FindYdbFixture fixture)
        : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
    }

    public class FindYdbTestSet(FindYdbFixture fixture) : FindYdbTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaSetFinder();
    }

    public class FindYdbTestContext(FindYdbFixture fixture) : FindYdbTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaContextFinder();
    }

    public class FindYdbTestNonGeneric(FindYdbFixture fixture) : FindYdbTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaNonGenericContextFinder();
    }

    public class FindYdbFixture : FindFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
