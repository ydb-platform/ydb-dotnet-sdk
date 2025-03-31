using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

#pragma warning disable xUnit1000
internal class TpcFiltersInheritanceBulkUpdatesYdbTest(
#pragma warning restore xUnit1000
    TPCFiltersInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : TPCFiltersInheritanceBulkUpdatesTestBase<TPCFiltersInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
{
    public override Task Delete_where_keyless_entity_mapped_to_sql_query(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_keyless_entity_mapped_to_sql_query,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_where_hierarchy(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_where_hierarchy_subquery(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_hierarchy_subquery,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First_3(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First_3,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_where_keyless_entity_mapped_to_sql_query(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_keyless_entity_mapped_to_sql_query,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "TODO: need fix")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_base_type(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_base_type,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_type_with_OfType(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_base_type_with_OfType,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_hierarchy_derived(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_using_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Delete_where_using_hierarchy_derived(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_using_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    // Base Test Ignored
    public override Task Delete_GroupBy_Where_Select_First(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First,
            Fixture.TestSqlLoggerFactory,
            async
        );

    // Base Test Ignored
    public override Task Delete_GroupBy_Where_Select_First_2(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First_2,
            Fixture.TestSqlLoggerFactory,
            async
        );

    // Base Test Ignored
    public override Task Update_where_hierarchy_subquery(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_hierarchy_subquery,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_property_on_derived_type(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_base_property_on_derived_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `k`.`Id`, `k`.`CountryId`, `k`.`Name`, `k`.`Species`, `k`.`EagleId`, `k`.`IsFlightless`, `k`.`FoundOn`
            FROM `Kiwi` AS `k`
            WHERE `k`.`CountryId` = 1
            """,
            """
            UPDATE `Kiwi`
            SET `Name` = 'SomeOtherKiwi'
            WHERE `CountryId` = 1
            """
        );

    public override Task Update_derived_property_on_derived_type(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_derived_property_on_derived_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `k`.`Id`, `k`.`CountryId`, `k`.`Name`, `k`.`Species`, `k`.`EagleId`, `k`.`IsFlightless`, `k`.`FoundOn`
            FROM `Kiwi` AS `k`
            WHERE `k`.`CountryId` = 1
            """,
            """
            UPDATE `Kiwi`
            SET `FoundOn` = 0
            WHERE `CountryId` = 1
            """
        );

    public override Task Update_base_and_derived_types(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_base_and_derived_types,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `k`.`Id`, `k`.`CountryId`, `k`.`Name`, `k`.`Species`, `k`.`EagleId`, `k`.`IsFlightless`, `k`.`FoundOn`
            FROM `Kiwi` AS `k`
            WHERE `k`.`CountryId` = 1
            """,
            """
            UPDATE `Kiwi`
            SET `FoundOn` = 0,
                `Name` = 'Kiwi'
            WHERE `CountryId` = 1
            """
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_using_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    [ConditionalTheory(Skip = "https://github.com/ydb-platform/ydb/issues/15177")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Update_where_using_hierarchy_derived(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_using_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
