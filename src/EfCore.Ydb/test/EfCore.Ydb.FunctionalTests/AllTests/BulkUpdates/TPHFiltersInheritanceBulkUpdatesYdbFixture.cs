namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

internal class TphFiltersInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
