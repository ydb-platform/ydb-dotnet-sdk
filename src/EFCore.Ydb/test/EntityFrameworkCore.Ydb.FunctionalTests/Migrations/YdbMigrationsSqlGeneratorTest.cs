using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Migrations;

public class YdbMigrationsSqlGeneratorTest() : MigrationsSqlGeneratorTestBase(YdbTestHelpers.Instance)
{
    public override void AddColumnOperation_with_fixed_length_no_model()
    {
        base.AddColumnOperation_with_fixed_length_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Name` TEXT;");
    }


    public override void AddColumnOperation_without_column_type()
    {
        base.AddColumnOperation_without_column_type();

        AssertSql("ALTER TABLE `People` ADD `Alias` TEXT NOT NULL;");
    }


    public override void AddColumnOperation_with_unicode_overridden()
    {
        base.AddColumnOperation_with_unicode_overridden();

        AssertSql("ALTER TABLE `Person` ADD `Name` TEXT;");
    }


    public override void AddColumnOperation_with_unicode_no_model()
    {
        base.AddColumnOperation_with_unicode_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Name` TEXT;");
    }


    public override void AddColumnOperation_with_maxLength_overridden()
    {
        base.AddColumnOperation_with_maxLength_overridden();

        AssertSql("ALTER TABLE `Person` ADD `Name` TEXT;");
    }


    public override void AddColumnOperation_with_maxLength_no_model()
    {
        base.AddColumnOperation_with_maxLength_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Name` TEXT;");
    }


    public override void AddColumnOperation_with_precision_and_scale_overridden()
    {
        base.AddColumnOperation_with_precision_and_scale_overridden();

        AssertSql("ALTER TABLE `Person` ADD `Pi` Decimal(15, 10) NOT NULL;");
    }


    public override void AddColumnOperation_with_precision_and_scale_no_model()
    {
        base.AddColumnOperation_with_precision_and_scale_no_model();

        AssertSql("ALTER TABLE `Person` ADD `Pi` Decimal(20, 7) NOT NULL;");
    }


    public override void AddForeignKeyOperation_without_principal_columns()
    {
        base.AddForeignKeyOperation_without_principal_columns();

        AssertSql(""); // Do nothing
    }


    public override void AlterColumnOperation_without_column_type() =>
        Assert.Throws<NotSupportedException>(() => base.AlterColumnOperation_without_column_type());

    public override void RenameTableOperation_legacy()
    {
        base.RenameTableOperation_legacy();

        AssertSql("ALTER TABLE `dbo/People` RENAME TO `dbo/Person`;");
    }

    public override void RenameTableOperation()
    {
        base.RenameTableOperation();

        AssertSql("ALTER TABLE `dbo/People` RENAME TO `dbo/Person`;");
    }

    public override void SqlOperation()
    {
        base.SqlOperation();

        AssertSql("-- I <3 DDL");
    }


    public override void InsertDataOperation_all_args_spatial() =>
        Assert.Throws<NotSupportedException>(() => base.InsertDataOperation_all_args_spatial());


    public override void InsertDataOperation_required_args()
    {
        base.InsertDataOperation_required_args();

        AssertSql(
            """
            INSERT INTO `dbo/People` (`First Name`)
            VALUES ('John'u);
            """);
    }


    public override void InsertDataOperation_required_args_composite()
    {
        base.InsertDataOperation_required_args_composite();

        AssertSql(
            """
            INSERT INTO `dbo/People` (`First Name`, `Last Name`)
            VALUES ('John'u, 'Snow'u);
            """);
    }


    public override void InsertDataOperation_required_args_multiple_rows()
    {
        base.InsertDataOperation_required_args_multiple_rows();

        AssertSql(
            """
            INSERT INTO `dbo/People` (`First Name`)
            VALUES ('John'u);
            INSERT INTO `dbo/People` (`First Name`)
            VALUES ('Daenerys'u);
            """);
    }


    public override void InsertDataOperation_throws_for_unsupported_column_types()
        => Assert.Equal(
            RelationalStrings.UnsupportedDataOperationStoreType("foo", "dbo.People.First Name"),
            Assert.Throws<InvalidOperationException>(() =>
                Generate(
                    new InsertDataOperation
                    {
                        Table = "People",
                        Schema = "dbo",
                        Columns = ["First Name"],
                        ColumnTypes = ["foo"],
                        Values = new object?[,] { { null } }
                    })).Message);

    public override void DeleteDataOperation_all_args()
    {
        base.DeleteDataOperation_all_args();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `First Name` = 'Hodor'u;
            DELETE FROM `People`
            WHERE `First Name` = 'Daenerys'u;
            DELETE FROM `People`
            WHERE `First Name` = 'John'u;
            DELETE FROM `People`
            WHERE `First Name` = 'Arya'u;
            DELETE FROM `People`
            WHERE `First Name` = 'Harry'u;
            """);
    }


    public override void DeleteDataOperation_all_args_composite()
    {
        base.DeleteDataOperation_all_args_composite();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `First Name` = 'Hodor'u AND `Last Name` IS NULL;
            DELETE FROM `People`
            WHERE `First Name` = 'Daenerys'u AND `Last Name` = 'Targaryen'u;
            DELETE FROM `People`
            WHERE `First Name` = 'John'u AND `Last Name` = 'Snow'u;
            DELETE FROM `People`
            WHERE `First Name` = 'Arya'u AND `Last Name` = 'Stark'u;
            DELETE FROM `People`
            WHERE `First Name` = 'Harry'u AND `Last Name` = 'Strickland'u;
            """);
    }


    public override void DeleteDataOperation_required_args()
    {
        base.DeleteDataOperation_required_args();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `Last Name` = 'Snow'u;
            """);
    }


    public override void DeleteDataOperation_required_args_composite()
    {
        base.DeleteDataOperation_required_args_composite();

        AssertSql(
            """
            DELETE FROM `People`
            WHERE `First Name` = 'John'u AND `Last Name` = 'Snow'u;
            """);
    }


    public override void UpdateDataOperation_all_args()
    {
        base.UpdateDataOperation_all_args();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Winterfell'u, `House Allegiance` = 'Stark'u, `Culture` = 'Northmen'u
            WHERE `First Name` = 'Hodor'u;
            UPDATE `People` SET `Birthplace` = 'Dragonstone'u, `House Allegiance` = 'Targaryen'u, `Culture` = 'Valyrian'u
            WHERE `First Name` = 'Daenerys'u;
            """);
    }


    public override void UpdateDataOperation_all_args_composite()
    {
        base.UpdateDataOperation_all_args_composite();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Stark'u
            WHERE `First Name` = 'Hodor'u AND `Last Name` IS NULL;
            UPDATE `People` SET `House Allegiance` = 'Targaryen'u
            WHERE `First Name` = 'Daenerys'u AND `Last Name` = 'Targaryen'u;
            """);
    }


    public override void UpdateDataOperation_all_args_composite_multi()
    {
        base.UpdateDataOperation_all_args_composite_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Winterfell'u, `House Allegiance` = 'Stark'u, `Culture` = 'Northmen'u
            WHERE `First Name` = 'Hodor'u AND `Last Name` IS NULL;
            UPDATE `People` SET `Birthplace` = 'Dragonstone'u, `House Allegiance` = 'Targaryen'u, `Culture` = 'Valyrian'u
            WHERE `First Name` = 'Daenerys'u AND `Last Name` = 'Targaryen'u;
            """);
    }


    public override void UpdateDataOperation_all_args_multi()
    {
        base.UpdateDataOperation_all_args_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Dragonstone'u, `House Allegiance` = 'Targaryen'u, `Culture` = 'Valyrian'u
            WHERE `First Name` = 'Daenerys'u;
            """);
    }


    public override void UpdateDataOperation_required_args()
    {
        base.UpdateDataOperation_required_args();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Targaryen'u
            WHERE `First Name` = 'Daenerys'u;
            """);
    }


    public override void UpdateDataOperation_required_args_multiple_rows()
    {
        base.UpdateDataOperation_required_args_multiple_rows();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Stark'u
            WHERE `First Name` = 'Hodor'u;
            UPDATE `People` SET `House Allegiance` = 'Targaryen'u
            WHERE `First Name` = 'Daenerys'u;
            """);
    }


    public override void UpdateDataOperation_required_args_composite()
    {
        base.UpdateDataOperation_required_args_composite();

        AssertSql(
            """
            UPDATE `People` SET `House Allegiance` = 'Targaryen'u
            WHERE `First Name` = 'Daenerys'u AND `Last Name` = 'Targaryen'u;
            """);
    }


    public override void UpdateDataOperation_required_args_composite_multi()
    {
        base.UpdateDataOperation_required_args_composite_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Dragonstone'u, `House Allegiance` = 'Targaryen'u, `Culture` = 'Valyrian'u
            WHERE `First Name` = 'Daenerys'u AND `Last Name` = 'Targaryen'u;
            """);
    }

    public override void UpdateDataOperation_required_args_multi()
    {
        base.UpdateDataOperation_required_args_multi();

        AssertSql(
            """
            UPDATE `People` SET `Birthplace` = 'Dragonstone'u, `House Allegiance` = 'Targaryen'u, `Culture` = 'Valyrian'u
            WHERE `First Name` = 'Daenerys'u;
            """
        );
    }

    public override void DefaultValue_with_line_breaks(bool isUnicode)
    {
        // YDB does not support default value
    }

    public override void DefaultValue_with_line_breaks_2(bool isUnicode)
    {
        // YDB does not support default value
    }

    public override void Sequence_restart_operation(long? startsAt)
    {
        // YDB does not support sequence
    }

    protected override string GetGeometryCollectionStoreType() => throw new NotSupportedException();
}
