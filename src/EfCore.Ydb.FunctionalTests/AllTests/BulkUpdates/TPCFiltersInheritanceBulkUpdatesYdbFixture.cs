namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TPCFiltersInheritanceBulkUpdatesYdbFixture : TPCInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters
        => true;
}
