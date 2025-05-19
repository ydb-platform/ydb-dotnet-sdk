namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

internal class TPTFiltersInheritanceBulkUpdatesYdbFixture : TPTInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters
        => true;
}
