namespace EntityFrameworkCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

internal class TpcFiltersInheritanceBulkUpdatesYdbFixture : TpcInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
