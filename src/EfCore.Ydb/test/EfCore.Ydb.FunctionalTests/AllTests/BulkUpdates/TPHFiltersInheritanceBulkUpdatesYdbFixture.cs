namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TphFiltersInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
