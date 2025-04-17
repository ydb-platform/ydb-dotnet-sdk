using System.Data;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Tests.Ado.Specification;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Ado;

[CollectionDefinition("YdbSchemaTests isolation test", DisableParallelization = true)]
[Collection("YdbSchemaTests isolation test")]
public class YdbSchemaTests : YdbAdoNetFixture
{
    private readonly string _table1;
    private readonly string _table2;
    private readonly string _table3;
    private readonly HashSet<string> _tableNames;

    public YdbSchemaTests(YdbFactoryFixture fixture) : base(fixture)
    {
        _table1 = $"a/b/{Utils.Net}_{Random.Shared.Next()}";
        _table2 = $"a/{Utils.Net}_{Random.Shared.Next()}";
        _table3 = $"{Utils.Net}_{Random.Shared.Next()}";
        _tableNames = new HashSet<string> { _table1, _table2, _table3 };
    }

    [Fact]
    public async Task GetSchema_WhenTablesCollection_ReturnAllTables()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        var table = await ydbConnection.GetSchemaAsync("Tables", new[] { null, "TABLE" });

        foreach (DataRow row in table.Rows)
        {
            _tableNames.Remove(row["table_name"].ToString()!);
        }

        Assert.Empty(_tableNames);

        var singleTable1 = await ydbConnection.GetSchemaAsync("Tables", new[] { _table1, "TABLE" });
        Assert.Equal(1, singleTable1.Rows.Count);
        Assert.Equal(_table1, singleTable1.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable1.Rows[0]["table_type"].ToString());

        var singleTable2 = await ydbConnection.GetSchemaAsync("Tables", new[] { _table2, null });
        Assert.Equal(1, singleTable2.Rows.Count);
        Assert.Equal(_table2, singleTable2.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable2.Rows[0]["table_type"].ToString());

        await Assert.ThrowsAsync<YdbException>(async () =>
            await ydbConnection.GetSchemaAsync("Tables", new[] { "not_found", null })
        );
    }

    [Fact]
    public async Task GetSchema_WhenTablesWithStatsCollection_ReturnAllTables()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        var table = await ydbConnection.GetSchemaAsync("TablesWithStats", new[] { null, "TABLE" });

        foreach (DataRow row in table.Rows)
        {
            _tableNames.Remove(row["table_name"].ToString()!);

            Assert.NotNull(row["rows_estimate"]);
            Assert.NotNull(row["creation_time"]);
            Assert.NotNull(row["modification_time"]);
        }

        Assert.Empty(_tableNames);

        var singleTable1 = await ydbConnection.GetSchemaAsync("TablesWithStats", new[] { _table1, "TABLE" });
        Assert.Equal(1, singleTable1.Rows.Count);
        Assert.Equal(_table1, singleTable1.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable1.Rows[0]["table_type"].ToString());
        Assert.NotNull(singleTable1.Rows[0]["rows_estimate"]);
        Assert.NotNull(singleTable1.Rows[0]["creation_time"]);
        Assert.NotNull(singleTable1.Rows[0]["modification_time"]);

        var singleTable2 = await ydbConnection.GetSchemaAsync("TablesWithStats", new[] { _table2, null });
        Assert.Equal(1, singleTable2.Rows.Count);
        Assert.Equal(_table2, singleTable2.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable2.Rows[0]["table_type"].ToString());
        Assert.NotNull(singleTable2.Rows[0]["rows_estimate"]);
        Assert.NotNull(singleTable2.Rows[0]["creation_time"]);
        Assert.NotNull(singleTable2.Rows[0]["modification_time"]);

        // not found case
        await Assert.ThrowsAsync<YdbException>(async () =>
            await ydbConnection.GetSchemaAsync("Tables", new[] { "not_found", null })
        );
    }

    [Fact]
    public async Task GetSchema_WhenColumnsCollection_ReturnAllColumns()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        foreach (var tableName in _tableNames)
        {
            var dataTable = await ydbConnection.GetSchemaAsync("Columns", new[] { tableName, null });

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

        var rowsA = (await ydbConnection.GetSchemaAsync("Columns", new[] { null, "a" })).Rows;
        Assert.Equal(3, rowsA.Count);
        for (var i = 0; i < rowsA.Count; i++)
        {
            CheckColumnA(rowsA[i]);
        }

        var rowsB = (await ydbConnection.GetSchemaAsync("Columns", new[] { null, "b" })).Rows;
        Assert.Equal(3, rowsB.Count);
        for (var i = 0; i < rowsB.Count; i++)
        {
            CheckColumnB(rowsB[i]);
        }

        foreach (var tableName in _tableNames)
        {
            var dataTable = await ydbConnection.GetSchemaAsync("Columns", new[] { tableName, "a" });
            Assert.Equal(1, dataTable.Rows.Count);
            var columnA = dataTable.Rows[0];
            Assert.Equal(tableName, columnA["table_name"]);
            CheckColumnA(columnA);
        }

        foreach (var tableName in _tableNames)
        {
            var dataTable = await ydbConnection.GetSchemaAsync("Columns", new[] { tableName, "b" });
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

    protected override async Task OnInitializeAsync()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = $@"
            CREATE TABLE `{_table1}` (a Int32 NOT NULL, b Int32, PRIMARY KEY(a));
            CREATE TABLE `{_table2}` (a Int32 NOT NULL, b Int32, PRIMARY KEY(a));
            CREATE TABLE `{_table3}` (a Int32 NOT NULL, b Int32, PRIMARY KEY(a));
            "
        }.ExecuteNonQueryAsync();
    }

    protected override async Task OnDisposeAsync()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = $@"
            DROP TABLE `{_table1}`; 
            DROP TABLE `{_table2}`; 
            DROP TABLE `{_table3}`;
            "
        }.ExecuteNonQueryAsync();
    }
}
