namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

internal class TPCFiltersInheritanceBulkUpdatesYdbFixture : TPCInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
