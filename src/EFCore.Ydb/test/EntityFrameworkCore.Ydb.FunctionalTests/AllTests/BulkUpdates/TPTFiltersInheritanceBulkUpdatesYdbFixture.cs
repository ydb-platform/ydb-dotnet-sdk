namespace EntityFrameworkCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

internal class TPTFiltersInheritanceBulkUpdatesYdbFixture : TPTInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters
        => true;
}
