using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class FiltersInheritanceQueryYdbTest : FiltersInheritanceQueryTestBase<TphFiltersInheritanceQueryYdbFixture>
{
    public FiltersInheritanceQueryYdbTest(TphFiltersInheritanceQueryYdbFixture fixture)
        : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }
}
