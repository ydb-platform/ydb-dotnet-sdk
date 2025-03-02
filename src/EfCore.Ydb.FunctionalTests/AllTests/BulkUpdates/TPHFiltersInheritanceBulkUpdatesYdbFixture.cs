namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TPHFiltersInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters
        => true;
}