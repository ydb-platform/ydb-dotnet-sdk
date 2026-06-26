namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class TphFiltersInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
