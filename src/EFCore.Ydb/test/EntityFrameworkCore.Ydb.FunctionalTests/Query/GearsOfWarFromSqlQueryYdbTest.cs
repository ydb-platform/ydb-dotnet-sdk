using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GearsOfWarFromSqlQueryYdbTest : GearsOfWarFromSqlQueryTestBase<GearsOfWarQueryYdbFixture>
{
    public GearsOfWarFromSqlQueryYdbTest(GearsOfWarQueryYdbFixture fixture, ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }
}
