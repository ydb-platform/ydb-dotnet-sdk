using System.Data;
using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
public class YdbCommandTests
{
    [Theory]
    [ClassData(typeof(YdbParameterTests.TestDataGenerator))]
    public async Task ExecuteScalarAsync_WhenSetYdbParameter_ReturnThisValue<T>(YdbParameterTests.Data<T> data)
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var dbCommand = connection.CreateCommand();

        dbCommand.CommandText = "SELECT $var;";

        var dbParameter = new YdbParameter
        {
            ParameterName = "$var",
            DbType = data.DbType,
            Value = data.Expected,
            IsNullable = data.IsNullable
        };

        dbCommand.Parameters.Add(dbParameter);

        Assert.Equal(data.Expected, await dbCommand.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WhenCreateTopic_ReturnEmptyResultSet()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "CREATE USER user PASSWORD '123qweqwe'";

        await dbCommand.ExecuteNonQueryAsync();

        dbCommand.CommandText = "DROP USER user;";
        await dbCommand.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task ExecuteDbDataReader_WhenSelectManyResultSet_ReturnYdbDataReader()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = @"
SELECT 1, CAST('text' AS Text);

$data = ListReplicate(AsStruct(true AS bool_field, 1.5 AS double_field, 23 AS int_field), 1500);

SELECT bool_field, double_field, int_field  FROM AS_TABLE($data);

SELECT CAST(NULL AS Int8) AS null_field;

$new_data = AsList(
    AsStruct($var1 AS Key, $var2 AS Value),
    AsStruct($var1 AS Key, $var2 AS Value)
);

SELECT Key, Value FROM AS_TABLE($new_data);
";

        var dateTime = new DateTime(2021, 08, 21, 23, 30, 47);
        var timestamp = DateTime.Parse("2029-08-03T06:59:44.8578730Z");

        dbCommand.Parameters.Add(new YdbParameter("$var1", DbType.DateTime, new DateTime(2021, 08, 21, 23, 30, 47)));
        dbCommand.Parameters.Add(new YdbParameter("$var2", DbType.DateTime2,
            DateTime.Parse("2029-08-03T06:59:44.8578730Z")));

        var ydbDataReader = await dbCommand.ExecuteReaderAsync();

        // Read 1 result set
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal("text", ydbDataReader.GetString(1));
        Assert.Equal("Ordinal must be between 0 and 1",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetValue(2)).Message);
        Assert.False(await ydbDataReader.ReadAsync());
        Assert.Equal("Invalid attempt to read when no data is present",
            Assert.Throws<InvalidOperationException>(() => ydbDataReader.GetValue(0)).Message);

        // Read 2 result set
        Assert.True(await ydbDataReader.NextResultAsync());
        for (var i = 0; i < 1500; i++)
        {
            Assert.True(await ydbDataReader.ReadAsync());

            Assert.Equal(true, ydbDataReader.GetValue("bool_field"));
            Assert.Equal(1.5, ydbDataReader.GetDouble(1));
            Assert.Equal(23, ydbDataReader.GetValue("int_field"));
        }

        Assert.False(await ydbDataReader.ReadAsync());

        // Read 3 result set
        Assert.True(await ydbDataReader.NextResultAsync());
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.True(ydbDataReader.IsDBNull(0));
        Assert.False(await ydbDataReader.ReadAsync());

        // Read 4 result set
        Assert.True(await ydbDataReader.NextResultAsync());
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.Equal(dateTime, ydbDataReader.GetDateTime(0));
        Assert.Equal(timestamp, ydbDataReader.GetDateTime(1));
        Assert.Equal("Field not found in row: non_existing_column",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetValue("non_existing_column")).Message);
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.Equal(dateTime, ydbDataReader.GetDateTime(0));
        Assert.Equal(timestamp, ydbDataReader.GetDateTime(1));
        Assert.False(await ydbDataReader.ReadAsync());
        Assert.False(await ydbDataReader.NextResultAsync());
        Assert.True(ydbDataReader.IsClosed);
    }

    [Fact]
    public void CommandTimeout_WhenCommandTimeoutLessZero_ThrowException()
    {
        using var connection = new YdbConnection();
        connection.Open();

        var dbCommand = connection.CreateCommand();

        Assert.Equal("CommandTimeout can't be less than zero. (Parameter 'value')\nActual value was -1.",
            Assert.Throws<ArgumentOutOfRangeException>(() => dbCommand.CommandTimeout = -1).Message);
    }

    [Fact]
    public void ExecuteDbDataReader_WhenPreviousIsNotClosed_ThrowException()
    {
        using var connection = new YdbConnection();
        connection.Open();

        var dbCommand = connection.CreateCommand();

        dbCommand.CommandText = "SELECT 1; SELECT 1;";

        var ydbDataReader = dbCommand.ExecuteReader();

        Assert.Equal("There is already an open YdbDataReader. " +
                     "Check if the previously opened YdbDataReader has been closed.",
            Assert.Throws<InvalidOperationException>(() => dbCommand.ExecuteReader()).Message);
        ydbDataReader.Close();
        Assert.True(ydbDataReader.IsClosed);
    }
}
