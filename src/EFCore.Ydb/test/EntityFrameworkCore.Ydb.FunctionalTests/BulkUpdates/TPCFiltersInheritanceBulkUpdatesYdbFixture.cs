namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

public class TpcFiltersInheritanceBulkUpdatesYdbFixture : TpcInheritanceBulkUpdatesYdbFixture
{
    public override bool EnableFilters => true;
}
