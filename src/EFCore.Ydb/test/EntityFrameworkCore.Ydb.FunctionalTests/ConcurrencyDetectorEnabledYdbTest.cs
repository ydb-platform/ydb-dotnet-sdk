using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class ConcurrencyDetectorEnabledYdbTest : ConcurrencyDetectorEnabledRelationalTestBase<
    ConcurrencyDetectorEnabledYdbTest.ConcurrencyDetectorYdbFixture>
{
    public ConcurrencyDetectorEnabledYdbTest(ConcurrencyDetectorYdbFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    public override Task FromSql(bool async)
        => ConcurrencyDetectorTest(async c => async
            ? await c.Products.FromSqlRaw(
                """
                select * from `Products`
                """).ToListAsync()
            : c.Products.FromSqlRaw(
                """
                select * from `Products`
                """).ToList());

    protected override async Task ConcurrencyDetectorTest(Func<ConcurrencyDetectorDbContext, Task<object>> test)
    {
        await base.ConcurrencyDetectorTest(test);

        Assert.Empty(Fixture.TestSqlLoggerFactory.SqlStatements);
    }

    public class ConcurrencyDetectorYdbFixture : ConcurrencyDetectorFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ListLoggerFactory;
    }
}
