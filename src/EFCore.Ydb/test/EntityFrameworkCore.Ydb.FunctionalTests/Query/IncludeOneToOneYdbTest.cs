using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class IncludeOneToOneYdbTest : IncludeOneToOneTestBase<IncludeOneToOneYdbTest.OneToOneQueryYdbFixture>
{
    public IncludeOneToOneYdbTest(OneToOneQueryYdbFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public class OneToOneQueryYdbFixture : OneToOneQueryFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();
    }
}
