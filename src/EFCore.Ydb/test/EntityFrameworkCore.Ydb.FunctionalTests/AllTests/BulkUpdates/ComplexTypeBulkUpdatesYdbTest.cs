using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

#pragma warning disable xUnit1000
internal class ComplexTypeBulkUpdatesYdbTest(
#pragma warning restore xUnit1000
    ComplexTypeBulkUpdatesYdbTest.ComplexTypeBulkUpdatesYdbFixture fixture,
    ITestOutputHelper testOutputHelper
) : ComplexTypeBulkUpdatesRelationalTestBase<ComplexTypeBulkUpdatesYdbTest.ComplexTypeBulkUpdatesYdbFixture>(
    fixture,
    testOutputHelper
)
{
    public override async Task Delete_entity_type_with_complex_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Delete_entity_type_with_complex_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            DELETE FROM `Customer`
            WHERE `Name` = 'Monty Elias'
            """
        );

    public override async Task Update_property_inside_complex_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_property_inside_complex_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            WHERE `c`.`ShippingAddress_ZipCode` = 7728
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_ZipCode` = 12345
            WHERE `ShippingAddress_ZipCode` = 7728
            """
        );

    public override async Task Update_property_inside_nested_complex_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_property_inside_nested_complex_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            WHERE `c`.`ShippingAddress_Country_Code` = 'US'
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_Country_FullName` = 'United States Modified'
            WHERE `ShippingAddress_Country_Code` = 'US'
            """
        );

    [ConditionalTheory(Skip = "Concatenation of strings is not implemented yet")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Update_multiple_properties_inside_multiple_complex_types_and_on_entity_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_multiple_properties_inside_multiple_complex_types_and_on_entity_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            UPDATE `Customer`
            SET "BillingAddress_ZipCode" = 54321,
                "ShippingAddress_ZipCode" = c."BillingAddress_ZipCode",
                "Name" = c."Name" || 'Modified'
            WHERE c."ShippingAddress_ZipCode" = 7728
            """
        );

    public override async Task Update_projected_complex_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_projected_complex_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_ZipCode` = 12345
            """
        );

    public override async Task Update_multiple_projected_complex_types_via_anonymous_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_multiple_projected_complex_types_via_anonymous_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            UPDATE `Customer`
            SET `BillingAddress_ZipCode` = 54321,
                `ShippingAddress_ZipCode` = `BillingAddress_ZipCode`
            """
        );

    public override async Task Update_complex_type_to_parameter(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_complex_type_to_parameter,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            $__complex_type_newAddress_0_AddressLine1='New AddressLine1'
            $__complex_type_newAddress_0_AddressLine2='New AddressLine2'
            $__complex_type_newAddress_0_Tags='["new_tag1","new_tag2"]'
            $__complex_type_newAddress_0_ZipCode='99999' (Nullable = true)
            $__complex_type_newAddress_0_Code='FR'
            $__complex_type_newAddress_0_FullName='France'

            UPDATE `Customer`
            SET `ShippingAddress_AddressLine1` = @__complex_type_newAddress_0_AddressLine1,
                `ShippingAddress_AddressLine2` = @__complex_type_newAddress_0_AddressLine2,
                `ShippingAddress_Tags` = @__complex_type_newAddress_0_Tags,
                `ShippingAddress_ZipCode` = @__complex_type_newAddress_0_ZipCode,
                `ShippingAddress_Country_Code` = @__complex_type_newAddress_0_Code,
                `ShippingAddress_Country_FullName` = @__complex_type_newAddress_0_FullName
            """
        );

    public override async Task Update_nested_complex_type_to_parameter(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_nested_complex_type_to_parameter,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            $__complex_type_newCountry_0_Code='FR'
            $__complex_type_newCountry_0_FullName='France'

            UPDATE `Customer`
            SET `ShippingAddress_Country_Code` = @__complex_type_newCountry_0_Code,
                `ShippingAddress_Country_FullName` = @__complex_type_newCountry_0_FullName
            """
        );

    public override async Task Update_complex_type_to_another_database_complex_type(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_complex_type_to_another_database_complex_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_AddressLine1` = `BillingAddress_AddressLine1`,
                `ShippingAddress_AddressLine2` = `BillingAddress_AddressLine2`,
                `ShippingAddress_Tags` = `BillingAddress_Tags`,
                `ShippingAddress_ZipCode` = `BillingAddress_ZipCode`,
                `ShippingAddress_Country_Code` = `ShippingAddress_Country_Code`,
                `ShippingAddress_Country_FullName` = `ShippingAddress_Country_FullName`
            """
        );

    public override async Task Update_complex_type_to_inline_without_lambda(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_complex_type_to_inline_without_lambda,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_AddressLine1` = 'New AddressLine1',
                `ShippingAddress_AddressLine2` = 'New AddressLine2',
                `ShippingAddress_Tags` = '["new_tag1","new_tag2"]',
                `ShippingAddress_ZipCode` = 99999,
                `ShippingAddress_Country_Code` = 'FR',
                `ShippingAddress_Country_FullName` = 'France'
            """
        );

    public override async Task Update_complex_type_to_inline_with_lambda(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_complex_type_to_inline_with_lambda,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_AddressLine1` = 'New AddressLine1',
                `ShippingAddress_AddressLine2` = 'New AddressLine2',
                `ShippingAddress_Tags` = '["new_tag1","new_tag2"]',
                `ShippingAddress_ZipCode` = 99999,
                `ShippingAddress_Country_Code` = 'FR',
                `ShippingAddress_Country_FullName` = 'France'
            """
        );

    [ConditionalTheory(Skip = "Inner query contains OFFSET without LIMIT. Impossible statement in YDB")]
    [MemberData(nameof(IsAsyncData))]
    public override async Task Update_complex_type_to_another_database_complex_type_with_subquery(bool async)
        => await SharedTestMethods.TestIgnoringBase(
            base.Update_complex_type_to_another_database_complex_type_with_subquery,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            @p='1'

            UPDATE `Customer`
            SET "ShippingAddress_AddressLine1" = c1."BillingAddress_AddressLine1",
                "ShippingAddress_AddressLine2" = c1."BillingAddress_AddressLine2",
                "ShippingAddress_Tags" = c1."BillingAddress_Tags",
                "ShippingAddress_ZipCode" = c1."BillingAddress_ZipCode",
                "ShippingAddress_Country_Code" = c1."ShippingAddress_Country_Code",
                "ShippingAddress_Country_FullName" = c1."ShippingAddress_Country_FullName"
            FROM (
                SELECT c."Id", c."BillingAddress_AddressLine1", c."BillingAddress_AddressLine2", c."BillingAddress_Tags", c."BillingAddress_ZipCode", c."ShippingAddress_Country_Code", c."ShippingAddress_Country_FullName"
                FROM `Customer` AS c
                ORDER BY c."Id" NULLS FIRST
                OFFSET @p
            ) AS c1
            WHERE c0."Id" = c1."Id"
            """);

    public override async Task Update_collection_inside_complex_type(bool async)
    {
        await SharedTestMethods.TestIgnoringBase(
            base.Update_collection_inside_complex_type,
            Fixture.TestSqlLoggerFactory,
            async,
            """
            SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
            FROM `Customer` AS `c`
            """,
            """
            UPDATE `Customer`
            SET `ShippingAddress_Tags` = '["new_tag1","new_tag2"]'
            """);

        AssertSql("""
                  SELECT `c`.`Id`, `c`.`Name`, `c`.`BillingAddress_AddressLine1`, `c`.`BillingAddress_AddressLine2`, `c`.`BillingAddress_Tags`, `c`.`BillingAddress_ZipCode`, `c`.`BillingAddress_Country_Code`, `c`.`BillingAddress_Country_FullName`, `c`.`ShippingAddress_AddressLine1`, `c`.`ShippingAddress_AddressLine2`, `c`.`ShippingAddress_Tags`, `c`.`ShippingAddress_ZipCode`, `c`.`ShippingAddress_Country_Code`, `c`.`ShippingAddress_Country_FullName`
                  FROM `Customer` AS `c`
                  """);
        AssertExecuteUpdateSql("""
                               UPDATE `Customer`
                               SET `ShippingAddress_Tags` = '["new_tag1","new_tag2"]'
                               """);
    }

    public class ComplexTypeBulkUpdatesYdbFixture : ComplexTypeBulkUpdatesRelationalFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;
    }

    private void AssertSql(params string[] expected) => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);

    private void AssertExecuteUpdateSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected, forUpdate: true);
}
