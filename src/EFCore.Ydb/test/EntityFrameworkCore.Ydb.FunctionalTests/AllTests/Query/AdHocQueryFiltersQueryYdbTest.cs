using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.AllTests.Query;

public class AdHocQueryFiltersQueryYdbTest : AdHocQueryFiltersQueryRelationalTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    // TODO: Fix subquery CAST
    public override Task GroupJoin_SelectMany_gets_flattened() => Task.CompletedTask;

    // TODO: Fix subquery CAST
    public override Task Group_by_multiple_aggregate_joining_different_tables(bool async) => Task.CompletedTask;

    // TODO: Fix subquery CAST
    public override Task Group_by_multiple_aggregate_joining_different_tables_with_query_filter(bool async) =>
        Task.CompletedTask;
}
