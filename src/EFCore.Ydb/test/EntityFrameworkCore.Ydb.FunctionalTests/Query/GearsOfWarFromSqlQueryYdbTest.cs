using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GearsOfWarFromSqlQueryYdbTest : GearsOfWarFromSqlQueryTestBase<GearsOfWarQueryYdbFixture>
{
    public GearsOfWarFromSqlQueryYdbTest(GearsOfWarQueryYdbFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }
}
