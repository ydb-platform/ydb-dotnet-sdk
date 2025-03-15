namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TPTFiltersInheritanceBulkUpdatesYdbFixture: TPTInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters
        => true;
}
