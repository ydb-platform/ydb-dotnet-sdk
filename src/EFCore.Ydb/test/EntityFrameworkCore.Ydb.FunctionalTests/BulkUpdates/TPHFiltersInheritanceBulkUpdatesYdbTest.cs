using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit;
using Xunit.Abstractions;
using static EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities.SharedTestMethods;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

// ReSharper disable once InconsistentNaming
public class TPHFiltersInheritanceBulkUpdatesYdbTest(
    TphFiltersInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : FiltersInheritanceBulkUpdatesRelationalTestBase<
    TphFiltersInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
{
    public override Task Delete_where_keyless_entity_mapped_to_sql_query(bool async)
        => AssertYdb(
            base.Delete_where_keyless_entity_mapped_to_sql_query,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_where_keyless_entity_mapped_to_sql_query(bool async)
        => AssertYdb(
            base.Update_where_keyless_entity_mapped_to_sql_query,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_where_hierarchy(bool async)
        => AssertYdb(
            base.Delete_where_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_where_hierarchy_subquery(bool async)
        => AssertYdb(
            base.Delete_where_hierarchy_subquery,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_where_hierarchy_derived(bool async)
        => AssertYdb(
            base.Delete_where_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy(bool async)
        => AssertYdb(
            base.Delete_where_using_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy_derived(bool async)
        => AssertYdb(
            base.Delete_where_using_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First(bool async)
        => AssertYdb(
            base.Delete_GroupBy_Where_Select_First,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First_2(bool async)
        => AssertYdb(
            base.Delete_GroupBy_Where_Select_First_2,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_GroupBy_Where_Select_First_3(bool async)
        => AssertYdb(
            base.Delete_GroupBy_Where_Select_First_3,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_type(bool async)
        => AssertYdb(
            base.Update_base_type,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_type_with_OfType(bool async)
        => AssertYdb(
            base.Update_base_type_with_OfType,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_where_hierarchy_subquery(bool async)
        => AssertYdb(
            base.Update_where_hierarchy_subquery,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_property_on_derived_type(bool async)
        => AssertYdb(
            base.Update_base_property_on_derived_type,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_derived_property_on_derived_type(bool async)
        => AssertYdb(
            base.Update_derived_property_on_derived_type,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_and_derived_types(bool async)
        => AssertYdb(
            base.Update_base_and_derived_types,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy(bool async)
        => AssertYdb(
            base.Update_where_using_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy_derived(bool async)
        => AssertYdb(
            base.Update_where_using_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
