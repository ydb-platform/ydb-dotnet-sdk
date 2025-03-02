using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TpcInheritanceBulkUpdatesYdbTest(
    TPCInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : TPCInheritanceBulkUpdatesTestBase<TPCInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
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

    public override Task Delete_where_hierarchy_derived(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            DELETE FROM `Kiwi`
            WHERE `Name` = 'Great spotted kiwi'
            """
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

    public override Task Delete_GroupBy_Where_Select_First(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_GroupBy_Where_Select_First_2(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First_2,
            Fixture.TestSqlLoggerFactory,
            async
        );

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
            """,
            """
            UPDATE `Kiwi`
            SET `Name` = 'SomeOtherKiwi'
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
            """,
            """
            UPDATE `Kiwi`
            SET `FoundOn` = 0
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
            """,
            """
            UPDATE `Kiwi`
            SET `FoundOn` = 0,
                `Name` = 'Kiwi'
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

    public override Task Update_with_interface_in_property_expression(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_with_interface_in_property_expression,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`SortIndex`, `c`.`CaffeineGrams`, `c`.`CokeCO2`, `c`.`SugarGrams`
            FROM `Coke` AS `c`
            """,
            """
            UPDATE `Coke`
            SET `SugarGrams` = 0
            """
        );

    public override Task Update_with_interface_in_EF_Property_in_property_expression(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_with_interface_in_EF_Property_in_property_expression,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`SortIndex`, `c`.`CaffeineGrams`, `c`.`CokeCO2`, `c`.`SugarGrams`
            FROM `Coke` AS `c`
            """,
            """
            UPDATE `Coke`
            SET `SugarGrams` = 0
            """
        );

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
