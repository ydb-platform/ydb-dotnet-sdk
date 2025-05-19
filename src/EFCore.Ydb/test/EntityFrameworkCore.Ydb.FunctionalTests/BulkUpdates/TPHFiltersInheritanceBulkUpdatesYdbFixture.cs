namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

internal class TphFiltersInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
