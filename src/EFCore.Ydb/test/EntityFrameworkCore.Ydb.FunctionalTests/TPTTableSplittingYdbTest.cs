using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class TptTableSplittingYdbTest(
#if !EFCORE9
    NonSharedFixture nonSharedFixture,
    #endif
    ITestOutputHelper testOutputHelper) : TPTTableSplittingTestBase(
#if !EFCORE9
    nonSharedFixture,
#endif
    testOutputHelper)
{
    // TODO: Should be fixed
    [ConditionalFact(Skip = "Sequence contains no elements` almost without stacktrace")]
    public override Task Can_insert_dependent_with_just_one_parent()
        => base.Can_insert_dependent_with_just_one_parent();

    [ConditionalTheory(Skip = "Requires fix")]
    [MemberData(nameof(IsAsyncData))]
    public override Task ExecuteUpdate_works_for_table_sharing(bool async)
        => base.ExecuteUpdate_works_for_table_sharing(async);

    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
