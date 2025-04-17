namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class TphFiltersInheritanceQueryYdbFixture : TphInheritanceQueryYdbFixture
{
    public override bool EnableFilters
        => true;
}
