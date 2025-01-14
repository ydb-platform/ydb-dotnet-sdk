using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[CollectionDefinition("YdbSchemaTests isolation test", DisableParallelization = true)]
[Collection("YdbSchemaTests isolation test")]
public class YdbSchemaTests
{
    [Fact]
    public async Task GetSchema_WhenTablesCollection_ReturnAllTables()
    {
        await using var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();

        var table1 = $"a/b/{Utils.Net}";
        var table2 = $"a/{Utils.Net}";
        var table3 = $"{Utils.Net}";

        var tableNames = new HashSet<string> { table1, table2, table3 };

        await new YdbCommand(ydbConnection)
        {
            CommandText = $@"
CREATE TABLE `{table1}` (a Int32, b Int32, PRIMARY KEY(a));
CREATE TABLE `{table2}` (a Int32, b Int32, PRIMARY KEY(a));
CREATE TABLE `{table3}` (a Int32, b Int32, PRIMARY KEY(a));
"
        }.ExecuteNonQueryAsync();

        var table = await ydbConnection.GetSchemaAsync("Tables", new[] { null, "TABLE" });
        Assert.Equal(3, table.Rows.Count);

        foreach (System.Data.DataRow row in table.Rows)
        {
            tableNames.Remove(row["table_name"].ToString()!);
        }

        Assert.Empty(tableNames);

        var singleTable1 = await ydbConnection.GetSchemaAsync("Tables", new[] { table1, "TABLE" });
        Assert.Equal(1, singleTable1.Rows.Count);
        Assert.Equal(table1, singleTable1.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable1.Rows[0]["table_type"].ToString());

        var singleTable2 = await ydbConnection.GetSchemaAsync("Tables", new[] { table2, null });
        Assert.Equal(1, singleTable2.Rows.Count);
        Assert.Equal(table2, singleTable2.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable2.Rows[0]["table_type"].ToString());

        // not found case
        var notFound = await ydbConnection.GetSchemaAsync("Tables", new[] { "not_found", null });
        Assert.Equal(0, notFound.Rows.Count);

        await new YdbCommand(ydbConnection)
        {
            CommandText = $@"
DROP TABLE `{table1}`; 
DROP TABLE `{table2}`; 
DROP TABLE `{table3}`;"
        }.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task GetSchema_WhenTablesWithStatsCollection_ReturnAllTables()
    {
        await using var ydbConnection = new YdbConnection();
        await ydbConnection.OpenAsync();

        var table1 = $"a/b/{Utils.Net}_for_stats";
        var table2 = $"a/{Utils.Net}_for_stats";
        var table3 = $"{Utils.Net}_for_stats";

        var tableNames = new HashSet<string> { table1, table2, table3 };

        await new YdbCommand(ydbConnection)
        {
            CommandText = $@"
CREATE TABLE `{table1}` (a Int32, b Int32, PRIMARY KEY(a));
CREATE TABLE `{table2}` (a Int32, b Int32, PRIMARY KEY(a));
CREATE TABLE `{table3}` (a Int32, b Int32, PRIMARY KEY(a));
"
        }.ExecuteNonQueryAsync();

        var table = await ydbConnection.GetSchemaAsync("TablesWithStats", new[] { null, "TABLE" });
        Assert.Equal(3, table.Rows.Count);

        foreach (System.Data.DataRow row in table.Rows)
        {
            tableNames.Remove(row["table_name"].ToString()!);

            Assert.Equal(0UL, row["rows_estimate"]);
            Assert.NotNull(row["creation_time"]);
            Assert.Equal(DBNull.Value, row["modification_time"]);
        }

        Assert.Empty(tableNames);

        var singleTable1 = await ydbConnection.GetSchemaAsync("TablesWithStats", new[] { table1, "TABLE" });
        Assert.Equal(1, singleTable1.Rows.Count);
        Assert.Equal(table1, singleTable1.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable1.Rows[0]["table_type"].ToString());
        Assert.Equal(0UL, singleTable1.Rows[0]["rows_estimate"]);
        Assert.NotNull(singleTable1.Rows[0]["creation_time"]);
        Assert.Equal(DBNull.Value, singleTable1.Rows[0]["modification_time"]);

        var singleTable2 = await ydbConnection.GetSchemaAsync("TablesWithStats", new[] { table2, null });
        Assert.Equal(1, singleTable2.Rows.Count);
        Assert.Equal(table2, singleTable2.Rows[0]["table_name"].ToString());
        Assert.Equal("TABLE", singleTable2.Rows[0]["table_type"].ToString());
        Assert.Equal(0UL, singleTable2.Rows[0]["rows_estimate"]);
        Assert.NotNull(singleTable2.Rows[0]["creation_time"]);
        Assert.Equal(DBNull.Value, singleTable2.Rows[0]["modification_time"]);

        // not found case
        var notFound = await ydbConnection.GetSchemaAsync("Tables", new[] { "not_found", null });
        Assert.Equal(0, notFound.Rows.Count);

        await new YdbCommand(ydbConnection)
        {
            CommandText = $@"
DROP TABLE `{table1}`; 
DROP TABLE `{table2}`; 
DROP TABLE `{table3}`;"
        }.ExecuteNonQueryAsync();
    }
}
