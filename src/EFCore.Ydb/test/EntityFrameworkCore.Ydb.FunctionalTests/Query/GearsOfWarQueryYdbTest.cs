using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Requires Time type and additional methods, contains correlated subqueries
internal class GearsOfWarQueryYdbTest
    : GearsOfWarQueryRelationalTestBase<GearsOfWarQueryYdbFixture>
{
    public GearsOfWarQueryYdbTest(
        GearsOfWarQueryYdbFixture fixture,
        ITestOutputHelper outputHelper
    ) : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
        fixture.TestSqlLoggerFactory.SetTestOutputHelper(outputHelper);
    }
}
