using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

// TODO: Refactor later
#pragma warning disable xUnit1000
internal class TPTFiltersInheritanceBulkUpdatesSqlServerTest(
#pragma warning restore xUnit1000
    TPTFiltersInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : TPTFiltersInheritanceBulkUpdatesTestBase<TPTFiltersInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
{
    // [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Delete_where_using_hierarchy(bool async)
    //     => Task.CompletedTask;

    // [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Delete_where_using_hierarchy_derived(bool async)
    //     => Task.CompletedTask;

    // [ConditionalTheory(Skip = "need fix")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Update_base_type(bool async)
    //     => Task.CompletedTask;
    //
    // [ConditionalTheory(Skip = "need fix")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Update_base_type_with_OfType(bool async)
    //     => Task.CompletedTask;

    // [ConditionalTheory(Skip = "need fix")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Update_base_property_on_derived_type(bool async)
    //     => Task.CompletedTask;

    // [ConditionalTheory(Skip = "need fix")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Update_derived_property_on_derived_type(bool async)
    //     => Task.CompletedTask;
    //
    // [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Update_where_using_hierarchy(bool async)
    //     => Task.CompletedTask;
    //
    // [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    // [MemberData(nameof(IsAsyncData))]
    // public override Task Update_where_using_hierarchy_derived(bool async)
    //     => Task.CompletedTask;

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
