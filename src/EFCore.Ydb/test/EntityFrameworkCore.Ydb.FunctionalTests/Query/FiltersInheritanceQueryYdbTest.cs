using Microsoft.EntityFrameworkCore.Query;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class FiltersInheritanceQueryYdbTest : FiltersInheritanceQueryTestBase<TphFiltersInheritanceQueryYdbFixture>
{
    public FiltersInheritanceQueryYdbTest(TphFiltersInheritanceQueryYdbFixture fixture,
        ITestOutputHelper testOutputHelper)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }
}
