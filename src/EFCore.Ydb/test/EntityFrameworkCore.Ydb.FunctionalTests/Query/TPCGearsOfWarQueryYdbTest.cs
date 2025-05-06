using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Requires Time type and additional methods, contains correlated subqueries
internal class TpcGearsOfWarQueryYdbTest : TPCGearsOfWarQueryRelationalTestBase<TPCGearsOfWarQueryYdbFixture>
{
    public TpcGearsOfWarQueryYdbTest(TPCGearsOfWarQueryYdbFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }
}
