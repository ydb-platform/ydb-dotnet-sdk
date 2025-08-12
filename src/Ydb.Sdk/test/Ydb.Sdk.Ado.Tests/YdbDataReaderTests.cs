using System.Data;
using Xunit;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

public class YdbDataReaderTests : TestBase
{
    [Fact]
    public async Task BasedIteration_WhenNotCallMethodRead_ThrowException()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(), codes.Add);

        // Read first metadata
        Assert.True(reader.HasRows);
        Assert.Equal(1, reader.FieldCount);
        Assert.Equal("Bool", reader.GetDataTypeName(0));

        Assert.Equal("No row is available", Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);

        Assert.Equal("No row is available",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message); // Need Read()

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));

        Assert.Equal("Ordinal must be between 0 and 0",
            Assert.Throws<IndexOutOfRangeException>(() => reader.GetBoolean(1)).Message);

        Assert.False(reader.Read());
        Assert.False(reader.IsClosed);

        Assert.Equal("No row is available",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);
        Assert.Empty(codes);

        await reader.CloseAsync();
        Assert.True(reader.IsClosed);
        Assert.Equal("The reader is closed",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);
        Assert.Equal("The reader is closed",
            Assert.Throws<InvalidOperationException>(() => reader.Read()).Message);
    }

    [Fact]
    public async Task CreateYdbDataReader_WhenAbortedStatus_ThrowException()
    {
        var codes = new List<StatusCode>();
        Assert.Equal("Status: Aborted", (await Assert.ThrowsAsync<YdbException>(() =>
            YdbDataReader.CreateYdbDataReader(SingleEnumeratorFailed, codes.Add))).Message);
        Assert.Single(codes);
        Assert.Equal(StatusCode.Aborted, codes[0]);
    }

    [Fact]
    public async Task NextResult_WhenNextResultSkipResultSet_ReturnNextResultSet()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2), codes.Add);

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.Read());
        Assert.False(reader.NextResult());
        Assert.Empty(codes);
    }

    [Fact]
    public async Task NextResult_WhenFirstRead_ReturnResultSet()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2), codes.Add);

        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.NextResult());
        Assert.False(reader.Read());
        Assert.Empty(codes);
    }

    [Fact]
    public async Task NextResult_WhenLongResultSet_ReturnResultSet()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2, true), codes.Add);

        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.NextResult());
        Assert.False(reader.Read());
        Assert.Empty(codes);
    }

    [Fact]
    public async Task Read_WhenReadAsyncThrowException_AggregateIssuesBeforeErrorAndAfter()
    {
        var codes = new List<StatusCode>();
        var result = ResultSet.Parser.ParseJson(
            "{ \"columns\": [ { \"name\": \"column0\", " +
            "\"type\": { \"typeId\": \"BOOL\" } } ], " +
            "\"rows\": [ { \"items\": [ { \"boolValue\": true } ] } ] }"
        );

        var successPart = new ExecuteQueryResponsePart
            { ResultSetIndex = 0, Status = StatusIds.Types.StatusCode.Success, ResultSet = result };
        successPart.Issues.Add(new IssueMessage { Message = "Some message 1" });

        var failPart = new ExecuteQueryResponsePart { Status = StatusIds.Types.StatusCode.Aborted };
        failPart.Issues.Add(new IssueMessage { Message = "Some message 2" });
        failPart.Issues.Add(new IssueMessage { Message = "Some message 2" });

        var nextFailPart = new ExecuteQueryResponsePart { Status = StatusIds.Types.StatusCode.Aborted };
        nextFailPart.Issues.Add(new IssueMessage { Message = "Some message 3" });

        var reader = await YdbDataReader.CreateYdbDataReader(new MockAsyncEnumerator<ExecuteQueryResponsePart>(
            new List<ExecuteQueryResponsePart> { successPart, failPart, nextFailPart }), codes.Add);

        Assert.True(reader.Read());
        Assert.Equal("""
                     Status: Aborted, Issues:
                     [0] Fatal: Some message 1
                     [0] Fatal: Some message 2
                     [0] Fatal: Some message 2
                     [0] Fatal: Some message 3
                     """, Assert.Throws<YdbException>(() => reader.Read()).Message);
        Assert.Single(codes);
        Assert.Equal(StatusCode.Aborted, codes[0]);
    }

    [Fact]
    public async Task GetEnumerator_WhenReadMultiSelect_ReadFirstResultSet()
    {
        await using var ydbConnection = await CreateOpenConnectionAsync();
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = @"
$new_data = AsList(
    AsStruct(1 AS Key, 'text' AS Value),
    AsStruct(1 AS Key, 'text' AS Value)
);

SELECT Key, Cast(Value AS Text) FROM AS_TABLE($new_data); SELECT 1, 'text';"
        };
        var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

        foreach (var row in ydbDataReader)
        {
            Assert.Equal(1, row.GetInt32(0));
            Assert.Equal("text", row.GetString(1));
        }

        Assert.True(ydbDataReader.NextResult());
        Assert.True(ydbDataReader.Read());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal("text"u8.ToArray(), ydbDataReader.GetBytes(1));
        Assert.False(ydbDataReader.Read());

        ydbDataReader = await ydbCommand.ExecuteReaderAsync();
        await foreach (var row in ydbDataReader)
        {
            Assert.Equal(1, row.GetInt32(0));
            Assert.Equal("text", row.GetString(1));
        }

        Assert.True(ydbDataReader.NextResult());
        Assert.True(ydbDataReader.Read());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal("text"u8.ToArray(), ydbDataReader.GetBytes(1));
        Assert.False(ydbDataReader.Read());
    }

    [Fact]
    public void GetChars_WhenSelectText_MoveCharsToBuffer()
    {
        using var connection = CreateOpenConnection();
        var ydbDataReader =
            new YdbCommand(connection) { CommandText = "SELECT CAST('abacaba' AS Text)" }.ExecuteReader();
        Assert.True(ydbDataReader.Read());
        var bufferChars = new char[10];
        var checkBuffer = new char[10];

        Assert.Equal(7, ydbDataReader.GetChars(0, 4, null, 0, 6));
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetChars(0, -1, null, 0, 6)).Message);
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(() =>
                ydbDataReader.GetChars(0, long.MaxValue, null, 0, 6)).Message);

        Assert.Equal("bufferOffset must be between 0 and 10",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetChars(0, 0, bufferChars, -1, 6)).Message);
        Assert.Equal("bufferOffset must be between 0 and 10",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetChars(0, 0, bufferChars, -1, 6)).Message);

        Assert.Equal("length must be between 0 and 10",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetChars(0, 0, bufferChars, 3, -1)).Message);
        Assert.Equal("bufferOffset must be between 0 and 5",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetChars(0, 0, bufferChars, 8, 5)).Message);

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
        Assert.False(ydbDataReader.Read());
    }

    [Fact]
    public void GetBytes_WhenSelectBytes_MoveBytesToBuffer()
    {
        using var connection = CreateOpenConnection();
        var ydbDataReader = new YdbCommand(connection) { CommandText = "SELECT 'abacaba'" }.ExecuteReader();
        Assert.True(ydbDataReader.Read());
        var bufferChars = new byte[10];
        var checkBuffer = new byte[10];

        Assert.Equal(7, ydbDataReader.GetBytes(0, 4, null, 0, 6));
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetBytes(0, -1, null, 0, 6)).Message);
        Assert.Equal($"dataOffset must be between 0 and {int.MaxValue}", Assert.Throws<IndexOutOfRangeException>(() =>
            ydbDataReader.GetBytes(0, long.MaxValue, null, 0, 6)).Message);

        Assert.Equal("bufferOffset must be between 0 and 10",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetBytes(0, 0, bufferChars, -1, 6)).Message);
        Assert.Equal("bufferOffset must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(() =>
            ydbDataReader.GetBytes(0, 0, bufferChars, -1, 6)).Message);

        Assert.Equal("length must be between 0 and 10", Assert.Throws<IndexOutOfRangeException>(() =>
            ydbDataReader.GetBytes(0, 0, bufferChars, 3, -1)).Message);
        Assert.Equal("bufferOffset must be between 0 and 5", Assert.Throws<IndexOutOfRangeException>(() =>
            ydbDataReader.GetBytes(0, 0, bufferChars, 8, 5)).Message);

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
        Assert.False(ydbDataReader.Read());
    }

    [Fact]
    public async Task ExecuteReaderAsync_WhenSelectManyResultSets_ReturnsCorrectDataAndMetaInfo()
    {
        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = @"
DECLARE $var1 AS Datetime;
DECLARE $var2 AS Timestamp;  
        
SELECT 1 as a, CAST('text' AS Text) as b;

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
        // Read meta info 
        Assert.Equal(2, ydbDataReader.FieldCount);
        Assert.True(ydbDataReader.HasRows);
        Assert.Equal("Int32", ydbDataReader.GetDataTypeName(0));
        Assert.Equal(0, ydbDataReader.GetOrdinal("a"));
        Assert.Equal(1, ydbDataReader.GetOrdinal("b"));
        Assert.Equal(typeof(int), ydbDataReader.GetFieldType(0));
        Assert.Equal(typeof(string), ydbDataReader.GetFieldType(1));
        Assert.Equal("a", ydbDataReader.GetName(0));
        Assert.Equal("b", ydbDataReader.GetName(1));

        // Read 1 result set
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.Equal(1, ydbDataReader.GetInt32(0));
        Assert.Equal("text", ydbDataReader.GetString(1));
        Assert.Equal("Ordinal must be between 0 and 1",
            Assert.Throws<IndexOutOfRangeException>(() => ydbDataReader.GetValue(2)).Message);
        Assert.False(await ydbDataReader.ReadAsync());
        Assert.Equal("No row is available",
            Assert.Throws<InvalidOperationException>(() => ydbDataReader.GetValue(0)).Message);

        Assert.True(ydbDataReader.HasRows);
        // Read 2 result set
        Assert.True(await ydbDataReader.NextResultAsync());
        Assert.Equal("Bool", ydbDataReader.GetDataTypeName(0));
        Assert.Equal("Double", ydbDataReader.GetDataTypeName(1));
        Assert.Equal("Int32", ydbDataReader.GetDataTypeName(2));
        for (var i = 0; i < 1500; i++)
        {
            // Read meta info 
            Assert.Equal(3, ydbDataReader.FieldCount);
            Assert.True(ydbDataReader.HasRows);
            Assert.Equal("int_field", ydbDataReader.GetName(2));

            Assert.True(await ydbDataReader.ReadAsync());

            Assert.Equal(true, ydbDataReader.GetValue("bool_field"));
            Assert.Equal(1.5, ydbDataReader.GetDouble(1));
            Assert.Equal(23, ydbDataReader.GetValue("int_field"));
        }

        Assert.False(await ydbDataReader.ReadAsync());

        // Read 3 result set
        Assert.True(await ydbDataReader.NextResultAsync());
        Assert.Equal("Int8", ydbDataReader.GetDataTypeName(0));
        Assert.Equal("null_field", ydbDataReader.GetName(0));
        Assert.True(await ydbDataReader.ReadAsync());
        Assert.True(ydbDataReader.IsDBNull(0));
        Assert.Equal(DBNull.Value, ydbDataReader.GetValue(0));
        Assert.False(await ydbDataReader.ReadAsync());

        // Read 4 result set
        Assert.True(await ydbDataReader.NextResultAsync());
        Assert.Equal("Datetime", ydbDataReader.GetDataTypeName(0));
        Assert.Equal("Key", ydbDataReader.GetName(0));
        Assert.Equal("Timestamp", ydbDataReader.GetDataTypeName(1));
        Assert.Equal("Value", ydbDataReader.GetName(1));
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
        Assert.False(ydbDataReader.IsClosed); // For IsClosed, invoke Close on YdbConnection or YdbDataReader.
    }

    private static MockAsyncEnumerator<ExecuteQueryResponsePart> EnumeratorSuccess(int size = 1,
        bool longFirstResultSet = false)
    {
        var result = ResultSet.Parser.ParseJson(
            "{ \"columns\": [ { \"name\": \"column0\", " +
            "\"type\": { \"typeId\": \"BOOL\" } } ], " +
            "\"rows\": [ { \"items\": [ { \"boolValue\": true } ] } ] }"
        );

        var list = new List<ExecuteQueryResponsePart>();

        if (longFirstResultSet)
        {
            list.Add(new ExecuteQueryResponsePart
                { ResultSetIndex = 0, Status = StatusIds.Types.StatusCode.Success, ResultSet = result });
        }

        for (var i = 0; i < size; i++)
        {
            list.Add(new ExecuteQueryResponsePart
                { ResultSetIndex = i, Status = StatusIds.Types.StatusCode.Success, ResultSet = result });
        }

        return new MockAsyncEnumerator<ExecuteQueryResponsePart>(list);
    }

    private static MockAsyncEnumerator<ExecuteQueryResponsePart> SingleEnumeratorFailed =>
        new(new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.Aborted } });
}
