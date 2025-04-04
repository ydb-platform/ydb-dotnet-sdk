using EfCore.Ydb.FunctionalTests.TestUtilities;
using EfCore.Ydb.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.FunctionalTests.Migrations;

public class YdbMigrationsTest : MigrationsTestBase<YdbMigrationsTest.YdbMigrationsFixture>
{
    public YdbMigrationsTest(YdbMigrationsFixture fixture, ITestOutputHelper testOutputHelper) : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override async Task Create_table()
    {
        await base.Create_table();

        AssertSql(
            """
            CREATE TABLE `People` (
                `Id` Serial NOT NULL,
                `Name` Text,
                PRIMARY KEY (`Id`)
            );
            """);
    }

    // Error: Primary key is required for ydb tables.
    public override Task Create_table_no_key() =>
        Assert.ThrowsAsync<YdbException>(() => base.Create_table_no_key());

    public override async Task Create_table_with_comments()
    {
        await base.Create_table_with_comments();

        AssertSql(
            """
            CREATE TABLE `People` (
                `Id` Serial NOT NULL,
                `Name` Text,
                PRIMARY KEY (`Id`)
            );
            """);
    }

    public override async Task Create_table_with_multiline_comments()
    {
        await base.Create_table_with_multiline_comments();

        AssertSql(
            """
            CREATE TABLE `People` (
                `Id` Serial NOT NULL,
                `Name` Text,
                PRIMARY KEY (`Id`)
            );
            """);
    }

    // YDB does not support comments
    protected override bool AssertComments => false;

    public override Task Create_table_with_computed_column(bool? stored) =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Create_table_with_computed_column(stored));

    public override async Task Drop_table()
    {
        await base.Drop_table();

        AssertSql("DROP TABLE `People`;");
    }

    public override Task Rename_json_column() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Rename_json_column());

    public override Task Rename_table_with_json_column() => Task.CompletedTask;

    public override async Task Rename_table()
    {
        await base.Rename_table();

        AssertSql("ALTER TABLE `People` RENAME TO `Persons`;");
    }

    public override async Task Rename_table_with_primary_key()
    {
        await base.Rename_table_with_primary_key();

        AssertSql("ALTER TABLE `People` RENAME TO `Persons`;");
    }

    public override Task Move_table() => Assert.ThrowsAsync<NotSupportedException>(() => base.Move_table());

    public override Task Create_schema() => Assert.ThrowsAsync<NotSupportedException>(() => base.Create_schema());

    public override Task Add_column_computed_with_collation(bool stored) =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Add_column_computed_with_collation(stored));

    public override Task Add_column_with_check_constraint() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_column_with_check_constraint());

    public override Task Add_json_columns_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_json_columns_to_existing_table());

    protected override bool AssertCollations => false;

    protected override bool AssertIndexFilters => false;

    // Error: Cannot add not null column without default value
    public override Task Add_column_with_defaultValue_string() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_column_with_defaultValue_string());

    public override Task Add_column_with_defaultValue_datetime() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_column_with_defaultValue_datetime());

    [Fact]
    public override Task Add_column_with_defaultValueSql() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_column_with_defaultValueSql());

    public override Task Add_column_with_computedSql(bool? stored) =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Add_column_with_computedSql(stored));

    public override Task Add_column_with_required() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_column_with_required());

    public override async Task Add_column_with_ansi()
    {
        await base.Add_column_with_ansi();

        AssertSql("ALTER TABLE `People` ADD `Name` Text;");
    }

    public override async Task Add_column_with_max_length()
    {
        await base.Add_column_with_max_length();

        AssertSql("ALTER TABLE `People` ADD `Name` Text;");
    }

    public override async Task Add_column_with_unbounded_max_length()
    {
        await base.Add_column_with_unbounded_max_length();

        AssertSql("ALTER TABLE `People` ADD `Name` Text;");
    }

    public override Task Add_column_with_max_length_on_derived() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_column_with_max_length_on_derived());

    public override async Task Add_column_with_fixed_length()
    {
        await base.Add_column_with_fixed_length();

        AssertSql("ALTER TABLE `People` ADD `Name` Text;");
    }

    public override async Task Add_column_with_comment()
    {
        await base.Add_column_with_comment();

        AssertSql("ALTER TABLE `People` ADD `FullName` Text;");
    }

    public override Task Alter_column_change_type() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_change_type());

    // AssertSql(
    //     """
    //     UPDATE "People" SET "SomeColumn" = '' WHERE "SomeColumn" IS NULL;
    //     ALTER TABLE "People" ALTER COLUMN "SomeColumn" SET NOT NULL;
    //     ALTER TABLE "People" ALTER COLUMN "SomeColumn" SET DEFAULT '';
    //     """);
    public override async Task Alter_column_make_required() =>
        await Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_required());

    [Fact]
    public override Task Alter_column_set_collation() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_column_set_collation());

    [Fact]
    public override Task Alter_column_reset_collation() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_column_reset_collation());

    public override Task Convert_string_column_to_a_json_column_containing_reference() =>
        Assert.ThrowsAsync<NotSupportedException>(() =>
            base.Convert_string_column_to_a_json_column_containing_reference());


    public override Task Convert_string_column_to_a_json_column_containing_required_reference() =>
        Assert.ThrowsAsync<NotSupportedException>(() =>
            base.Convert_string_column_to_a_json_column_containing_required_reference());

    public override Task Convert_string_column_to_a_json_column_containing_collection() =>
        Assert.ThrowsAsync<NotSupportedException>(() =>
            base.Convert_string_column_to_a_json_column_containing_collection());

    public override async Task Drop_column()
    {
        await base.Drop_column();

        AssertSql("ALTER TABLE `People` DROP COLUMN `SomeColumn`;");
    }

    public override Task Drop_column_primary_key() =>
        Assert.ThrowsAsync<YdbException>(() => base.Drop_column_primary_key());

    public override Task Rename_column() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Rename_column());

    public override Task Create_index_unique()
        => Assert.ThrowsAsync<YdbException>(() => base.Create_index_unique());

    public override Task Add_required_primitive_collection_with_custom_default_value_sql_to_existing_table() =>
        Task.CompletedTask;

    public override Task Add_required_primitve_collection_with_custom_default_value_sql_to_existing_table() =>
        Task.CompletedTask;

    public override Task Add_required_primitive_collection_with_custom_default_value_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() =>
            base.Add_required_primitive_collection_with_custom_default_value_to_existing_table());

    public override async Task Create_index()
    {
        await base.Create_index();

        AssertSql("ALTER TABLE `People` ADD INDEX `IX_People_FirstName` GLOBAL SYNC ON (`FirstName`);");
    }

    public override async Task Drop_index()
    {
        await base.Drop_index();

        AssertSql("ALTER TABLE `People` DROP INDEX `IX_People_SomeField`;");
    }

    public override async Task Rename_index()
    {
        await base.Rename_index();

        AssertSql("ALTER TABLE `People` RENAME INDEX `Foo` TO `foo`;");
    }

    // Error: Primary key is required for ydb tables.
    public override Task Add_primary_key_int() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_primary_key_int());

    // Error: Primary key is required for ydb tables.
    public override Task Add_primary_key_string() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_primary_key_string());

    public override Task Add_primary_key_with_name() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_primary_key_with_name());

    public override Task Add_primary_key_composite_with_name() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_primary_key_composite_with_name());

    public override Task Drop_primary_key_int() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Drop_primary_key_int());

    public override Task Drop_primary_key_string() => Task.CompletedTask;

    public override Task Add_required_primitive_collection_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_required_primitive_collection_to_existing_table());

    public override Task
        Add_required_primitive_collection_with_custom_converter_and_custom_default_value_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() =>
            base.Add_required_primitive_collection_with_custom_converter_and_custom_default_value_to_existing_table()
        );

    public override Task Add_required_primitve_collection_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_required_primitve_collection_to_existing_table());

    public override Task
        Add_required_primitve_collection_with_custom_converter_and_custom_default_value_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() =>
            base.Add_required_primitve_collection_with_custom_converter_and_custom_default_value_to_existing_table());

    public override Task Add_required_primitve_collection_with_custom_default_value_to_existing_table() =>
        Assert.ThrowsAsync<YdbException>(() =>
            base.Add_required_primitve_collection_with_custom_default_value_to_existing_table());

    public override Task Alter_check_constraint() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_check_constraint());

    public override Task Alter_column_add_comment() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_add_comment());

    public override Task Alter_column_change_comment() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_change_comment());

    public override Task Alter_column_change_computed() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_change_computed());

    public override Task Alter_column_change_computed_recreates_indexes() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_change_computed_recreates_indexes());

    public override Task Alter_column_change_computed_type() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_change_computed_type());

    public override Task Alter_column_make_computed(bool? stored) =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_computed(stored));

    public override Task Alter_column_make_non_computed() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_non_computed());

    public override Task Alter_column_make_required_with_composite_index() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_required_with_composite_index());

    public override Task Alter_column_make_required_with_index() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_required_with_index());

    public override Task Alter_column_make_required_with_null_data() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_make_required_with_null_data());

    public override Task Alter_column_remove_comment() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_column_remove_comment());

    public override Task Alter_computed_column_add_comment() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_computed_column_add_comment());

    public override Task Alter_index_change_sort_order() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_index_change_sort_order());

    public override Task Alter_index_make_unique() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_index_make_unique());

    public override Task Alter_table_add_comment_non_default_schema() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Alter_table_add_comment_non_default_schema());

    public override Task Add_foreign_key() => Task.CompletedTask;

    public override Task Add_foreign_key_with_name() => Task.CompletedTask;

    public override Task Drop_foreign_key() => Task.CompletedTask;

    public override Task Add_unique_constraint() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_unique_constraint());

    public override Task Add_unique_constraint_composite_with_name() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_unique_constraint_composite_with_name());

    public override async Task Drop_unique_constraint()
    {
        await base.Drop_unique_constraint();

        AssertSql("ALTER TABLE `People` DROP INDEX `AK_People_AlternateKeyColumn`;");
    }

    public override Task Add_check_constraint_with_name() =>
        Assert.ThrowsAsync<YdbException>(() => base.Add_check_constraint_with_name());

    public override Task Drop_check_constraint() =>
        Assert.ThrowsAsync<YdbException>(() => base.Drop_check_constraint());

    public override Task Create_sequence() =>
        Assert.ThrowsAsync<YdbException>(() => base.Create_sequence());

    public override Task Create_sequence_all_settings() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Create_sequence_all_settings());

    public override Task Create_sequence_long() =>
        Assert.ThrowsAsync<YdbException>(() => base.Create_sequence_long());

    public override Task Create_sequence_short() =>
        Assert.ThrowsAsync<YdbException>(() => base.Create_sequence_short());

    public override Task Create_table_all_settings() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Create_table_all_settings());

    public override async Task Create_table_with_complex_type_with_required_properties_on_derived_entity_in_TPH()
    {
        await Test((Action<ModelBuilder>)(_ => { }), (Action<ModelBuilder>)(builder =>
        {
            builder.Entity("Contact", e =>
            {
                e.Property<int>("Id").ValueGeneratedOnAdd();
                e.HasKey("Id");
                e.Property<string>("Name");
                e.ToTable("Contacts");
            });
            builder.Entity("Supplier", e =>
            {
                e.HasBaseType("Contact");
                e.Property<int>("Number");
                e.ComplexProperty<MyComplex>("MyComplex",
                    ct => ct.ComplexProperty<MyNestedComplex>("MyNestedComplex").IsRequired());
            });
        }), (Action<DatabaseModel>)(model => Assert.Collection(
            Assert.Single(model.Tables, t => t.Name == "Contacts").Columns,
            c =>
            {
                Assert.Equal("MyComplex_MyNestedComplex_Foo", c.Name);
                Assert.True(c.IsNullable);
            },
            c => Assert.Equal("Id", c.Name),
            c => Assert.Equal("Discriminator", c.Name),
            c => Assert.Equal("Name", c.Name),
            c => Assert.Equal("Number", c.Name),
            c =>
            {
                Assert.Equal("MyComplex_Prop", c.Name);
                Assert.True(c.IsNullable);
            },
            c =>
            {
                Assert.Equal("MyComplex_MyNestedComplex_Bar", c.Name);
                Assert.True(c.IsNullable);
            })));

        AssertSql(
            """
            CREATE TABLE `Contacts` (
                `Id` Serial NOT NULL,
                `Discriminator` Text NOT NULL,
                `Name` Text,
                `Number` Int32,
                `MyComplex_Prop` Text,
                `MyComplex_MyNestedComplex_Bar` Timestamp,
                `MyComplex_MyNestedComplex_Foo` Int32,
                PRIMARY KEY (`Id`)
            );
            """);
    }

    public override Task Create_unique_index_with_filter() =>
        Assert.ThrowsAsync<YdbException>(() => base.Create_unique_index_with_filter());

    // YDB does not support
    public override Task Create_index_descending() => Task.CompletedTask;

    // YDB does not support
    public override Task Create_index_descending_mixed() => Task.CompletedTask;

    public override Task Drop_column_computed_and_non_computed_with_dependency() =>
        Assert.ThrowsAsync<NotSupportedException>(() => base.Drop_column_computed_and_non_computed_with_dependency());

    public override Task Alter_sequence_all_settings() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_sequence_all_settings());

    public override Task Alter_sequence_increment_by() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_sequence_increment_by());

    public override Task Alter_sequence_restart_with() =>
        Assert.ThrowsAsync<YdbException>(() => base.Alter_sequence_restart_with());

    public override Task Drop_sequence() =>
        Assert.ThrowsAsync<YdbException>(() => base.Drop_sequence());

    public override Task Rename_sequence() =>
        Assert.ThrowsAsync<YdbException>(() => base.Rename_sequence());

    public override Task Move_sequence() =>
        Assert.ThrowsAsync<YdbException>(() => base.Move_sequence());

    public override async Task InsertDataOperation()
    {
        await base.InsertDataOperation();

        AssertSql(
            """
            INSERT INTO `Person` (`Id`, `Name`)
            VALUES (1, 'Daenerys Targaryen');
            INSERT INTO `Person` (`Id`, `Name`)
            VALUES (2, 'John Snow');
            INSERT INTO `Person` (`Id`, `Name`)
            VALUES (3, 'Arya Stark');
            INSERT INTO `Person` (`Id`, `Name`)
            VALUES (4, 'Harry Strickland');
            INSERT INTO `Person` (`Id`, `Name`)
            VALUES (5, NULL);
            """
        );
    }

    public override async Task DeleteDataOperation_simple_key()
    {
        await base.DeleteDataOperation_simple_key();

        AssertSql(
            """
            DELETE FROM `Person`
            WHERE `Id` = 2;
            """);
    }

    public override async Task DeleteDataOperation_composite_key()
    {
        await base.DeleteDataOperation_composite_key();

        AssertSql(
            """
            DELETE FROM `Person`
            WHERE `AnotherId` = 12 AND `Id` = 2;
            """);
    }

    public override async Task UpdateDataOperation_simple_key()
    {
        await base.UpdateDataOperation_simple_key();

        AssertSql(
            """
            UPDATE `Person` SET `Name` = 'Another John Snow'
            WHERE `Id` = 2;
            """);
    }

    public override async Task UpdateDataOperation_composite_key()
    {
        await base.UpdateDataOperation_composite_key();

        AssertSql(
            """
            UPDATE `Person` SET `Name` = 'Another John Snow'
            WHERE `AnotherId` = 11 AND `Id` = 2;
            """);
    }

    public override async Task UpdateDataOperation_multiple_columns()
    {
        await base.UpdateDataOperation_multiple_columns();

        AssertSql(
            """
            UPDATE `Person` SET `Age` = 21, `Name` = 'Another John Snow'
            WHERE `Id` = 2;
            """);
    }

    public override Task SqlOperation() => Assert.ThrowsAsync<YdbException>(() => base.SqlOperation());

    protected override string NonDefaultCollation => "collaction";

    public class YdbMigrationsFixture : MigrationsFixtureBase
    {
        protected override string StoreName => nameof(YdbMigrationsTest);

        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

        public override RelationalTestHelpers TestHelpers => YdbTestHelpers.Instance;

        protected override IServiceCollection AddServices(IServiceCollection serviceCollection) =>
            base.AddServices(serviceCollection)
                .AddScoped<IDatabaseModelFactory, YdbDatabaseModelFactory>();
    }
}
