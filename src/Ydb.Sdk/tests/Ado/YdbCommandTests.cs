using System.Data;
using System.Text;
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
    public async Task ExecuteNonQueryAsync_WhenCreateUser_ReturnEmptyResultSet()
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
    public async Task CloseAsync_WhenDoubleInvoke_Idempotent()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1;";
        var ydbDataReader = ydbCommand.ExecuteReader();

        Assert.True(await ydbDataReader.NextResultAsync());
        await ydbDataReader.CloseAsync();
        await ydbDataReader.CloseAsync();
        Assert.False(await ydbDataReader.NextResultAsync());
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

        Assert.Equal("A command is already in progress: SELECT 1; SELECT 1;",
            Assert.Throws<YdbOperationInProgressException>(() => dbCommand.ExecuteReader()).Message);
        ydbDataReader.Close();
        Assert.True(ydbDataReader.IsClosed);
    }

    [Fact]
    public void GetChars_WhenSelectText_MoveCharsToBuffer()
    {
        using var connection = new YdbConnection();
        connection.Open();
        var ydbDataReader =
            new YdbCommand(connection) { CommandText = "SELECT CAST('abacaba' AS Text)" }.ExecuteReader();
        Assert.True(ydbDataReader.Read());
        var bufferChars = new char[10];
        var checkBuffer = new char[10];

        Assert.Equal(0, ydbDataReader.GetChars(0, 4, null, 0, 6));
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetChars(0, -1, null, 0, 6)).Message);
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(
                () => ydbDataReader.GetChars(0, long.MaxValue, null, 0, 6)).Message);

        Assert.Equal("bufferOffset must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetChars(0, 0, bufferChars, -1, 6)).Message);
        Assert.Equal("bufferOffset must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetChars(0, 0, bufferChars, -1, 6)).Message);

        Assert.Equal("length must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetChars(0, 0, bufferChars, 3, -1)).Message);
        Assert.Equal("bufferOffset must be between 0 and 5", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetChars(0, 0, bufferChars, 8, 5)).Message);

        Assert.Equal(6, ydbDataReader.GetChars(0, 0, bufferChars, 4, 6));
        checkBuffer[4] = 'a';
        checkBuffer[5] = 'b';
        checkBuffer[6] = 'a';
        checkBuffer[7] = 'c';
        checkBuffer[8] = 'a';
        checkBuffer[9] = 'b';
        Assert.Equal(checkBuffer, bufferChars);
        bufferChars = new char[10];
        checkBuffer = new char[10];

        Assert.Equal(4, ydbDataReader.GetChars(0, 3, bufferChars, 4, 6));
        checkBuffer[4] = 'c';
        checkBuffer[5] = 'a';
        checkBuffer[6] = 'b';
        checkBuffer[7] = 'a';
        Assert.Equal(checkBuffer, bufferChars);

        Assert.Equal('a', ydbDataReader.GetChar(0));
    }

    [Fact]
    public void GetBytes_WhenSelectBytes_MoveBytesToBuffer()
    {
        using var connection = new YdbConnection();
        connection.Open();
        var ydbDataReader = new YdbCommand(connection) { CommandText = "SELECT 'abacaba'" }.ExecuteReader();
        Assert.True(ydbDataReader.Read());
        var bufferChars = new byte[10];
        var checkBuffer = new byte[10];

        Assert.Equal(0, ydbDataReader.GetBytes(0, 4, null, 0, 6));
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetBytes(0, -1, null, 0, 6)).Message);
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(
                () => ydbDataReader.GetBytes(0, long.MaxValue, null, 0, 6)).Message);

        Assert.Equal("bufferOffset must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetBytes(0, 0, bufferChars, -1, 6)).Message);
        Assert.Equal("bufferOffset must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetBytes(0, 0, bufferChars, -1, 6)).Message);

        Assert.Equal("length must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetBytes(0, 0, bufferChars, 3, -1)).Message);
        Assert.Equal("bufferOffset must be between 0 and 5", Assert.Throws<IndexOutOfRangeException>(
            () => ydbDataReader.GetBytes(0, 0, bufferChars, 8, 5)).Message);

        Assert.Equal(6, ydbDataReader.GetBytes(0, 0, bufferChars, 4, 6));
        checkBuffer[4] = (byte)'a';
        checkBuffer[5] = (byte)'b';
        checkBuffer[6] = (byte)'a';
        checkBuffer[7] = (byte)'c';
        checkBuffer[8] = (byte)'a';
        checkBuffer[9] = (byte)'b';
        Assert.Equal(checkBuffer, bufferChars);
        bufferChars = new byte[10];
        checkBuffer = new byte[10];

        Assert.Equal(4, ydbDataReader.GetBytes(0, 3, bufferChars, 4, 5));
        checkBuffer[4] = (byte)'c';
        checkBuffer[5] = (byte)'a';
        checkBuffer[6] = (byte)'b';
        checkBuffer[7] = (byte)'a';
        Assert.Equal(checkBuffer, bufferChars);
    }

    [Fact]
    public async Task GetEnumerator_WhenReadMultiSelect_ReadFirstResultSet()
    {
        await using var ydbConnection = new YdbConnection();
        ydbConnection.Open();
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = @"
$new_data = AsList(
    AsStruct(1 AS Key, 'text' AS Value),
    AsStruct(1 AS Key, 'text' AS Value)
);

SELECT Key, Cast(Value AS Text) FROM AS_TABLE($new_data); SELECT 1, 'text';"
        };
        var ydbDataReader = ydbCommand.ExecuteReader();

        foreach (var row in ydbDataReader)
        {
            Assert.Equal(1, row.GetInt32(0));
            Assert.Equal("text", row.GetString(1));
        }

        Assert.True(ydbDataReader.NextResult());
        Assert.True(ydbDataReader.Read());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal(Encoding.ASCII.GetBytes("text"), ydbDataReader.GetBytes(1));
        Assert.False(ydbDataReader.Read());

        ydbDataReader = ydbCommand.ExecuteReader();
        await foreach (var row in ydbDataReader)
        {
            Assert.Equal(1, row.GetInt32(0));
            Assert.Equal("text", row.GetString(1));
        }

        Assert.True(ydbDataReader.NextResult());
        Assert.True(ydbDataReader.Read());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal(Encoding.ASCII.GetBytes("text"), ydbDataReader.GetBytes(1));
        Assert.False(ydbDataReader.Read());
    }
}
