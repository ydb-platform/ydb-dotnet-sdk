using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

/// <summary>
/// Tests for Gears of War query scenarios in YDB provider.
/// Note: Some tests require Time data type support and have issues with correlated subqueries.
/// These are YDB server limitations that affect complex query patterns.
/// </summary>
public class GearsOfWarQueryYdbTest
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
