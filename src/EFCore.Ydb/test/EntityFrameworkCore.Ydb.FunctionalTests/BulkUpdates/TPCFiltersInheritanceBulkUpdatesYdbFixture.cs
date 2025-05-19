namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

internal class TpcFiltersInheritanceBulkUpdatesYdbFixture : TpcInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
