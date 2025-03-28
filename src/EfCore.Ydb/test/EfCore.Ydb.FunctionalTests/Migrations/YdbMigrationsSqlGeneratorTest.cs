using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EfCore.Ydb.FunctionalTests.Migrations;

public class YdbMigrationsSqlGeneratorTest() : MigrationsSqlGeneratorTestBase(YdbTestHelpers.Instance)
{
    [ConditionalFact]
    public override void AddColumnOperation_with_fixed_length_no_model()
    {
        base.AddColumnOperation_with_fixed_length_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Name` Text;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_without_column_type()
    {
        base.AddColumnOperation_without_column_type();

        AssertSql("ALTER TABLE `People` ADD `Alias` Text NOT NULL;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_with_unicode_overridden()
    {
        base.AddColumnOperation_with_unicode_overridden();

        AssertSql("ALTER TABLE `Person` ADD `Name` Text;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_with_unicode_no_model()
    {
        base.AddColumnOperation_with_unicode_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Name` Text;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_with_maxLength_overridden()
    {
        base.AddColumnOperation_with_maxLength_overridden();

        AssertSql("ALTER TABLE `Person` ADD `Name` Text;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_with_maxLength_no_model()
    {
        base.AddColumnOperation_with_maxLength_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Name` Text;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_with_precision_and_scale_overridden()
    {
        base.AddColumnOperation_with_precision_and_scale_overridden();

        AssertSql("ALTER TABLE `Person` ADD `Pi` Decimal(15, 10) NOT NULL;");
    }

    [ConditionalFact]
    public override void AddColumnOperation_with_precision_and_scale_no_model()
    {
        base.AddColumnOperation_with_precision_and_scale_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Pi` Decimal(20, 7) NOT NULL;");
    }

    [ConditionalFact]
    public override void AddForeignKeyOperation_without_principal_columns()
    {
        base.AddForeignKeyOperation_without_principal_columns();

        AssertSql(""); // Do nothing
    }

    [ConditionalFact]
    public override void AlterColumnOperation_without_column_type() =>
        Assert.Throws<NotSupportedException>(() => base.AlterColumnOperation_without_column_type());

    [ConditionalFact]
    public override void RenameTableOperation_legacy()
    {
        base.RenameTableOperation_legacy();

        AssertSql("ALTER TABLE `dbo/People` RENAME TO `dbo/Person`;");
    }

    [ConditionalFact]
    public override void RenameTableOperation()
    {
        base.RenameTableOperation();

        AssertSql("ALTER TABLE `dbo/People` RENAME TO `dbo/Person`;");
    }

    [ConditionalFact]
    public override void SqlOperation()
    {
        base.SqlOperation();

        AssertSql("-- I <3 DDL");
    }

    [ConditionalFact]
    public override void InsertDataOperation_all_args_spatial() =>
        Assert.Throws<NotSupportedException>(() => base.InsertDataOperation_all_args_spatial());

    [ConditionalFact]
    public override void InsertDataOperation_required_args()
    {
        base.InsertDataOperation_required_args();

        AssertSql(
            """
            INSERT INTO `dbo.People` ("First Name")
            VALUES ('John');
            """);
    }

    [ConditionalFact]
    public override void InsertDataOperation_required_args_composite()
    {
        base.InsertDataOperation_required_args_composite();

        AssertSql(
            """
            INSERT INTO `dbo/People` (`First Name`, `Last Name`)
            VALUES ('John', 'Snow');
            """);
    }

    [ConditionalFact]
    public override void InsertDataOperation_required_args_multiple_rows()
    {
        base.InsertDataOperation_required_args_multiple_rows();

        AssertSql(
            """
            INSERT INTO `dbo/People` (`First Name`)
            VALUES ('John');
            INSERT INTO `dbo/People` (`First Name`)
            VALUES ('Daenerys');
            """);
    }

    [ConditionalFact(Skip = "TBD")]
    public override void InsertDataOperation_throws_for_unsupported_column_types()
    {
        base.InsertDataOperation_throws_for_unsupported_column_types();
    }

    [ConditionalFact]
    public override void DeleteDataOperation_all_args()
    {
        base.DeleteDataOperation_all_args();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `First Name` = 'Hodor';
            DELETE FROM `People`
            WHERE `First Name` = 'Daenerys';
            DELETE FROM `People`
            WHERE `First Name` = 'John';
            DELETE FROM `People`
            WHERE `First Name` = 'Arya';
            DELETE FROM `People`
            WHERE `First Name` = 'Harry';
            """);
    }

    [ConditionalFact]
    public override void DeleteDataOperation_all_args_composite()
    {
        base.DeleteDataOperation_all_args_composite();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `First Name` = 'Hodor' AND `Last Name` IS NULL;
            DELETE FROM `People`
            WHERE `First Name` = 'Daenerys' AND `Last Name` = 'Targaryen';
            DELETE FROM `People`
            WHERE `First Name` = 'John' AND `Last Name` = 'Snow';
            DELETE FROM `People`
            WHERE `First Name` = 'Arya' AND `Last Name` = 'Stark';
            DELETE FROM `People`
            WHERE `First Name` = 'Harry' AND `Last Name` = 'Strickland';
            """);
    }

    [ConditionalFact]
    public override void DeleteDataOperation_required_args()
    {
        base.DeleteDataOperation_required_args();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `Last Name` = 'Snow';
            """);
    }

    [ConditionalFact]
    public override void DeleteDataOperation_required_args_composite()
    {
        base.DeleteDataOperation_required_args_composite();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `First Name` = 'John' AND `Last Name` = 'Snow';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_all_args()
    {
        base.UpdateDataOperation_all_args();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Winterfell', `House Allegiance` = 'Stark', `Culture` = 'Northmen'
            WHERE `First Name` = 'Hodor';
            UPDATE `People` SET `Birthplace` = 'Dragonstone', `House Allegiance` = 'Targaryen', `Culture` = 'Valyrian'
            WHERE `First Name` = 'Daenerys';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_all_args_composite()
    {
        base.UpdateDataOperation_all_args_composite();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Stark'
            WHERE `First Name` = 'Hodor' AND `Last Name` IS NULL;
            UPDATE `People` SET `House Allegiance` = 'Targaryen'
            WHERE `First Name` = 'Daenerys' AND `Last Name` = 'Targaryen';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_all_args_composite_multi()
    {
        base.UpdateDataOperation_all_args_composite_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Winterfell', `House Allegiance` = 'Stark', `Culture` = 'Northmen'
            WHERE `First Name` = 'Hodor' AND `Last Name` IS NULL;
            UPDATE `People` SET `Birthplace` = 'Dragonstone', `House Allegiance` = 'Targaryen', `Culture` = 'Valyrian'
            WHERE `First Name` = 'Daenerys' AND `Last Name` = 'Targaryen';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_all_args_multi()
    {
        base.UpdateDataOperation_all_args_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Dragonstone', `House Allegiance` = 'Targaryen', `Culture` = 'Valyrian'
            WHERE `First Name` = 'Daenerys';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_required_args()
    {
        base.UpdateDataOperation_required_args();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Targaryen'
            WHERE `First Name` = 'Daenerys';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_required_args_multiple_rows()
    {
        base.UpdateDataOperation_required_args_multiple_rows();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Stark'
            WHERE `First Name` = 'Hodor';
            UPDATE `People` SET `House Allegiance` = 'Targaryen'
            WHERE `First Name` = 'Daenerys';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_required_args_composite()
    {
        base.UpdateDataOperation_required_args_composite();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Targaryen'
            WHERE `First Name` = 'Daenerys' AND `Last Name` = 'Targaryen';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_required_args_composite_multi()
    {
        base.UpdateDataOperation_required_args_composite_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Dragonstone', `House Allegiance` = 'Targaryen', `Culture` = 'Valyrian'
            WHERE `First Name` = 'Daenerys' AND `Last Name` = 'Targaryen';
            """);
    }

    [ConditionalFact]
    public override void UpdateDataOperation_required_args_multi()
    {
        base.UpdateDataOperation_required_args_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Dragonstone', `House Allegiance` = 'Targaryen', `Culture` = 'Valyrian'
            WHERE `First Name` = 'Daenerys';
            """
        );
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override void DefaultValue_with_line_breaks(bool isUnicode)
    {
        base.DefaultValue_with_line_breaks(isUnicode);

        AssertSql("""
                  CREATE TABLE `dbo/TestLineBreaks` (
                      `TestDefaultValue` Text NOT NULL
                  );
                  """);
    }

    [ConditionalTheory]
    [InlineData(false)]
    [InlineData(true)]
    public override void DefaultValue_with_line_breaks_2(bool isUnicode)
    {
        base.DefaultValue_with_line_breaks_2(isUnicode);
    }

    [ConditionalTheory(Skip = "ClickHouse does not support sequences")]
    public override void Sequence_restart_operation(long? startsAt)
    {
        base.Sequence_restart_operation(startsAt);
    }

    protected override string GetGeometryCollectionStoreType() => throw new NotSupportedException();
}
