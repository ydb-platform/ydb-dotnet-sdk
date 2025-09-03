using System.Data;
using Xunit;

namespace Ydb.Sdk.Ado.Tests;

[Collection("YdbSchemaTests isolation test")]
[CollectionDefinition("YdbSchemaTests isolation test", DisableParallelization = true)]
public class YdbSchemaTests : TestBase
{
    private readonly string _table1;
    private readonly string _table2;
    private readonly string _table3;
    private readonly string _allTypesTable;
    private readonly string _allTypesTableNullable;
    private readonly HashSet<string> _allTableNames;
    private readonly HashSet<string> _simpleTableNames;

    public YdbSchemaTests()
    {
        _table1 = $"a/b/table_{Random.Shared.Next()}";
        _table2 = $"a/table_{Random.Shared.Next()}";
        _table3 = $"table_{Random.Shared.Next()}";
        _allTypesTable = $"allTypesTable_{Random.Shared.Next()}";
        _allTypesTableNullable = $"allTypesTableNullable_{Random.Shared.Next()}";
        _allTableNames = [_table1, _table2, _table3, _allTypesTable, _allTypesTableNullable];
        _simpleTableNames = [_table1, _table2, _table3];
    }

    [Fact]
    public async Task GetSchema_WhenTablesCollection_ReturnAllTables()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        var table = await ydbConnection.GetSchemaAsync("Tables", [null, "TABLE"]);

        foreach (DataRow row in table.Rows)
        {
            _allTableNames.Remove(row["table_name"].ToString()!);
        }

        Assert.Empty(_allTableNames);

        var singleTable1 = await ydbConnection.GetSchemaAsync("Tables", [_table1, "TABLE"]);
        Assert.Equal(1, singleTable1.Rows.Count);
        Assert.Equal(_table1, singleTable1.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable1.Rows[0]["table_type"].ToString());

        var singleTable2 = await ydbConnection.GetSchemaAsync("Tables", [_table2, null]);
        Assert.Equal(1, singleTable2.Rows.Count);
        Assert.Equal(_table2, singleTable2.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable2.Rows[0]["table_type"].ToString());

        await Assert.ThrowsAsync<YdbException>(async () =>
            await ydbConnection.GetSchemaAsync("Tables", ["not_found", null])
        );
    }

    [Fact]
    public async Task GetSchema_WhenTablesWithStatsCollection_ReturnAllTables()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        var table = await ydbConnection.GetSchemaAsync("TablesWithStats", [null, "TABLE"]);

        foreach (DataRow row in table.Rows)
        {
            _allTableNames.Remove(row["table_name"].ToString()!);

            Assert.NotNull(row["rows_estimate"]);
            Assert.NotNull(row["creation_time"]);
            Assert.NotNull(row["modification_time"]);
        }

        Assert.Empty(_allTableNames);

        var singleTable1 = await ydbConnection.GetSchemaAsync("TablesWithStats", [_table1, "TABLE"]);
        Assert.Equal(1, singleTable1.Rows.Count);
        Assert.Equal(_table1, singleTable1.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable1.Rows[0]["table_type"].ToString());
        Assert.NotNull(singleTable1.Rows[0]["rows_estimate"]);
        Assert.NotNull(singleTable1.Rows[0]["creation_time"]);
        Assert.NotNull(singleTable1.Rows[0]["modification_time"]);

        var singleTable2 = await ydbConnection.GetSchemaAsync("TablesWithStats", [_table2, null]);
        Assert.Equal(1, singleTable2.Rows.Count);
        Assert.Equal(_table2, singleTable2.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable2.Rows[0]["table_type"].ToString());
        Assert.NotNull(singleTable2.Rows[0]["rows_estimate"]);
        Assert.NotNull(singleTable2.Rows[0]["creation_time"]);
        Assert.NotNull(singleTable2.Rows[0]["modification_time"]);

        // not found case
        await Assert.ThrowsAsync<YdbException>(async () =>
            await ydbConnection.GetSchemaAsync("Tables", ["not_found", null])
        );
    }

    [Fact]
    public async Task GetSchema_WhenColumnsCollection_ReturnAllColumns()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        foreach (var tableName in new[] { _table1, _table2, _table3 })
        {
            var dataTable = await ydbConnection.GetSchemaAsync("Columns", [tableName, null]);

            Assert.Equal(2, dataTable.Rows.Count);

            const int ordinalA = 0;
            var columnA = dataTable.Rows[ordinalA];
            Assert.Equal(tableName, columnA["table_name"]);
            CheckColumnA(columnA);

            const int ordinalB = 1;
            var columnB = dataTable.Rows[ordinalB];
            Assert.Equal(tableName, columnB["table_name"]);
            CheckColumnB(columnB);
        }

        var rowsA = (await ydbConnection.GetSchemaAsync("Columns", [null, "a"])).Rows;
        Assert.Equal(3, rowsA.Count);
        for (var i = 0; i < rowsA.Count; i++)
        {
            CheckColumnA(rowsA[i]);
        }

        var rowsB = (await ydbConnection.GetSchemaAsync("Columns", [null, "b"])).Rows;
        Assert.Equal(3, rowsB.Count);
        for (var i = 0; i < rowsB.Count; i++)
        {
            CheckColumnB(rowsB[i]);
        }

        foreach (var tableName in _simpleTableNames)
        {
            var dataTable = await ydbConnection.GetSchemaAsync("Columns", [tableName, "a"]);
            Assert.Equal(1, dataTable.Rows.Count);
            var columnA = dataTable.Rows[0];
            Assert.Equal(tableName, columnA["table_name"]);
            CheckColumnA(columnA);
        }

        foreach (var tableName in _simpleTableNames)
        {
            var dataTable = await ydbConnection.GetSchemaAsync("Columns", [tableName, "b"]);
            Assert.Equal(1, dataTable.Rows.Count);
            var columnB = dataTable.Rows[0];
            Assert.Equal(tableName, columnB["table_name"]);
            CheckColumnB(columnB);
        }

        return;

        void CheckColumnA(DataRow columnA)
        {
            Assert.Equal("a", columnA["column_name"]);
            Assert.Equal(0, columnA["ordinal_position"]);
            Assert.Equal("NO", columnA["is_nullable"]);
            Assert.Equal("Int32", columnA["data_type"]);
            Assert.Empty((string)columnA["family_name"]);
        }

        void CheckColumnB(DataRow columnB)
        {
            Assert.Equal("b", columnB["column_name"]);
            Assert.Equal(1, columnB["ordinal_position"]);
            Assert.Equal("YES", columnB["is_nullable"]);
            Assert.Equal("Int32", columnB["data_type"]);
            Assert.Empty((string)columnB["family_name"]);
        }
    }

    [Fact]
    public async Task GetSchema_WhenAllTypesTable_ReturnAllTypes()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var dataTable = await ydbConnection.GetSchemaAsync("Columns", [_allTypesTable, null]);
        var dataTableNullable = await ydbConnection.GetSchemaAsync("Columns", [_allTypesTableNullable, null]);

        Assert.Equal(17, dataTable.Rows.Count);
        Assert.Equal(17, dataTableNullable.Rows.Count);

        CheckAllColumns(dataTable, false);
        CheckAllColumns(dataTableNullable, true);
        return;

        void CheckAllColumns(DataTable pDataTable, bool isNullableTable)
        {
            CheckColumn(pDataTable.Rows[0], "Int32Column", 0, isNullableTable);
            CheckColumn(pDataTable.Rows[1], "BoolColumn", 1, isNullableTable);
            CheckColumn(pDataTable.Rows[2], "Int64Column", 2, isNullableTable);
            CheckColumn(pDataTable.Rows[3], "Int16Column", 3, isNullableTable);
            CheckColumn(pDataTable.Rows[4], "Int8Column", 4, isNullableTable);
            CheckColumn(pDataTable.Rows[5], "FloatColumn", 5, isNullableTable);
            CheckColumn(pDataTable.Rows[6], "DoubleColumn", 6, isNullableTable);
            CheckColumn(pDataTable.Rows[7], "DefaultDecimalColumn", 7, isNullableTable, "Decimal(22, 9)");
            CheckColumn(pDataTable.Rows[8], "Uint8Column", 8, isNullableTable);
            CheckColumn(pDataTable.Rows[9], "Uint16Column", 9, isNullableTable);
            CheckColumn(pDataTable.Rows[10], "Uint32Column", 10, isNullableTable);
            CheckColumn(pDataTable.Rows[11], "Uint64Column", 11, isNullableTable);
            CheckColumn(pDataTable.Rows[12], "TextColumn", 12, isNullableTable);
            CheckColumn(pDataTable.Rows[13], "BytesColumn", 13, isNullableTable);
            CheckColumn(pDataTable.Rows[14], "DateColumn", 14, isNullableTable);
            CheckColumn(pDataTable.Rows[15], "DatetimeColumn", 15, isNullableTable);
            CheckColumn(pDataTable.Rows[16], "TimestampColumn", 16, isNullableTable);
        }

        void CheckColumn(DataRow column, string columnName, int ordinal, bool isNullable, string? dataType = null)
        {
            Assert.Equal(columnName, column["column_name"]);
            Assert.Equal(ordinal, column["ordinal_position"]);
            Assert.Equal(isNullable ? "YES" : "NO", column["is_nullable"]);
            Assert.Equal(dataType ?? columnName[..^"Column".Length], column["data_type"]);
            Assert.Empty((string)column["family_name"]);
        }
    }

    protected override async Task OnInitializeAsync()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           CREATE TABLE `{_table1}` (a Int32 NOT NULL, b Int32, PRIMARY KEY(a));
                           CREATE TABLE `{_table2}` (a Int32 NOT NULL, b Int32, PRIMARY KEY(a));
                           CREATE TABLE `{_table3}` (a Int32 NOT NULL, b Int32, PRIMARY KEY(a));

                           CREATE TABLE {_allTypesTable} (
                               Int32Column Int32 NOT NULL,
                               BoolColumn Bool NOT NULL,
                               Int64Column Int64 NOT NULL,
                               Int16Column Int16 NOT NULL,
                               Int8Column Int8 NOT NULL,
                               FloatColumn Float NOT NULL,
                               DoubleColumn Double NOT NULL,
                               DefaultDecimalColumn Decimal(22,9) NOT NULL,
                               Uint8Column Uint8 NOT NULL,
                               Uint16Column Uint16 NOT NULL,
                               Uint32Column Uint32 NOT NULL,
                               Uint64Column Uint64 NOT NULL,
                               TextColumn Text NOT NULL,
                               BytesColumn Bytes NOT NULL,
                               DateColumn Date NOT NULL,
                               DatetimeColumn Datetime NOT NULL,
                               TimestampColumn Timestamp NOT NULL,
                               PRIMARY KEY (Int32Column)
                           );

                           CREATE TABLE {_allTypesTableNullable} (
                               Int32Column Int32,
                               BoolColumn Bool,
                               Int64Column Int64,
                               Int16Column Int16,
                               Int8Column Int8,
                               FloatColumn Float,
                               DoubleColumn Double,
                               DefaultDecimalColumn Decimal(22,9),
                               Uint8Column Uint8,
                               Uint16Column Uint16,
                               Uint32Column Uint32,
                               Uint64Column Uint64,
                               TextColumn Text,
                               BytesColumn Bytes,
                               DateColumn Date,
                               DatetimeColumn Datetime,
                               TimestampColumn Timestamp,
                               PRIMARY KEY (Int32Column)
                           );
                           """
        }.ExecuteNonQueryAsync();
    }

    protected override async Task OnDisposeAsync()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           DROP TABLE `{_table1}`; 
                           DROP TABLE `{_table2}`; 
                           DROP TABLE `{_table3}`;
                           DROP TABLE `{_allTypesTable}`;
                           DROP TABLE `{_allTypesTableNullable}`;
                           """
        }.ExecuteNonQueryAsync();
    }
}
