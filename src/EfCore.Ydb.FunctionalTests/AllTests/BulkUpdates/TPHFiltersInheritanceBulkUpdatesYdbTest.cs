using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

// TODO: following error
// Error: Primary key is required for ydb tables.
// Probably use Name+CountryId, but...
class TPHFiltersInheritanceBulkUpdatesYdbTest(
    TPHFiltersInheritanceBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : FiltersInheritanceBulkUpdatesRelationalTestBase<
    TPHFiltersInheritanceBulkUpdatesYdbFixture>(fixture, testOutputHelper)
{
    public override Task Delete_where_keyless_entity_mapped_to_sql_query(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_keyless_entity_mapped_to_sql_query,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_where_keyless_entity_mapped_to_sql_query(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_keyless_entity_mapped_to_sql_query,
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

    public override Task Delete_where_hierarchy_derived(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Delete_where_using_hierarchy(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_where_using_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

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

    public override Task Delete_GroupBy_Where_Select_First_3(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Delete_GroupBy_Where_Select_First_3,
            Fixture.TestSqlLoggerFactory,
            async
        );

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
            async
        );

    public override Task Update_derived_property_on_derived_type(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_derived_property_on_derived_type,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_base_and_derived_types(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_base_and_derived_types,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_where_using_hierarchy(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_using_hierarchy,
            Fixture.TestSqlLoggerFactory,
            async
        );

    public override Task Update_where_using_hierarchy_derived(bool async)
        => SharedTestMethods.TestIgnoringBase(
            base.Update_where_using_hierarchy_derived,
            Fixture.TestSqlLoggerFactory,
            async
        );

    protected override void ClearLog()
        => Fixture.TestSqlLoggerFactory.Clear();
}
