using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
#if !EFCORE9
using Microsoft.EntityFrameworkCore;
#endif

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class AdHocQueryFiltersQueryYdbTest : AdHocQueryFiltersQueryRelationalTestBase
{
#if !EFCORE9
    public AdHocQueryFiltersQueryYdbTest(NonSharedFixture fixture) : base(fixture)
    {
    }
#endif

    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

#if EFCORE9
    public override Task GroupJoin_SelectMany_gets_flattened() => Task.CompletedTask;
#endif
    // TODO: Fix subquery CAST
    public override Task Group_by_multiple_aggregate_joining_different_tables(bool async) => Task.CompletedTask;

    // TODO: Fix subquery CAST
    public override Task Group_by_multiple_aggregate_joining_different_tables_with_query_filter(bool async) =>
        Task.CompletedTask;
}
